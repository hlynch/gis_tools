import rasterio.features
import rasterio
import rasterio.warp
import fiona
import multiprocessing
from collections import namedtuple
from rasterio.crs import CRS
from rasterio.warp import transform_bounds
import traceback
import sys
Footprint = namedtuple('Footprint',('crs','geom','filename','meta'))
import os
import datetime


class DigitalGlobeSchema:
    def __init__(self,tags):
        self.raw_tags = tags

    def parse_tags(self):
        try:
            tags = self.raw_tags
            
            # get sensor or replace with UNK for unknown sensor
            sensor = tags.get('NITF_PIAIMC_SENSNAME', 'UNK')

            # get acquisition date or replace with 1st Jan 9999
            date = tags.get('NITF_STDIDC_ACQUISITION_DATE', '99990101000000')
            date = datetime.datetime.strptime(date, '%Y%m%d%H%M%S')
            date_str = str(date)

            return {'sensor': sensor,
                    'date': date_str,
                    'year': date.year,
                    'month': date.month,
                    'day': date.day}
        except:
            return {'sensor': 'UNK',
                    'date': str(datetime.datetime.strptime('00000000000000', '%Y%m%d%H%M%S')),
                    'year': '9999',
                    'month': '1',
                    'day': '1'}




class BoundingBox:
    def __init__(self,bounds,crs):
        bbox = transform_bounds(crs,{'init': 'epsg:4326'}, *bounds)
        self.bbox = bbox

    def __repr__(self):
        return self.bbox.__repr__()

    def to_geometry(self):
        bbox = self.bbox
        return {'type': 'Polygon',
                'coordinates': [[
                    [bbox[0], bbox[1]],
                    [bbox[2], bbox[1]],
                    [bbox[2], bbox[3]],
                    [bbox[0], bbox[3]],
                    [bbox[0], bbox[1]]]]}


class Tiff:

    def __init__(self, tiff):

        self.file = tiff

        self.mask = None
        self.transform = None
        self.crs = None
        self.bounds = None
        self.meta_data = {}

        self.extract_meta_data()

        self.meta_data.update({'location':tiff})

    def extract_meta_data(self):
        with rasterio.open(self.file) as input_raster:
            self.transform = input_raster.transform
            self.crs = input_raster.crs.copy()
            self.bounds = input_raster.bounds
            self.meta_data = DigitalGlobeSchema(input_raster.tags()).parse_tags()

    def extract_mask(self):
        with rasterio.open(self.file) as input_raster:
            self.mask = input_raster.read_masks(1)

    def maskgeometry(self):

        if self.mask is None:
            self.extract_mask()

        geom = rasterio.features.shapes(self.mask, mask=self.mask, transform=self.transform)
        geoms = []

        with rasterio.Env(OGR_ENABLE_PARTIAL_REPROJECTION=True):
            for i, (g, val) in enumerate(geom):
                g = rasterio.warp.transform_geom(
                    self.crs, 'EPSG:4326', g,
                    antimeridian_cutting=False)
                geoms.append(g)

        meta_data = self.meta_data
        meta_data['type'] = 'data_mask'
        return Footprint(self.crs, geoms[0], self.file, meta_data)

    def boundingbox(self):
        meta_data = self.meta_data
        meta_data['type'] = 'bbox'
        geom = BoundingBox(self.bounds, self.crs).to_geometry()
        return Footprint(self.crs, geom, self.file, meta_data)


def extract_footprint(tiff, bb=False):

    tif = Tiff(tiff)

    if bb:
        return tif.boundingbox()
    else:
        try:
            return tif.maskgeometry()
        except Exception as e:
            with open(log_file, 'a') as log:
                print(tiff," Error:", e, " Trying bounding box",file=log)
            return tif.boundingbox()


def extract_footprint_worker(tiff, result_queue,bb=False):
    footprint = extract_footprint(tiff,bb)
    result_queue.put(footprint)
    return tiff


def write_footprint(shapefile, crs, result_queue, log_file):
    '''
    listens for messages on the results queue, writes shapefile.
    '''
    sf = ShapefileWriter(shapefile)
    while True:
        try:
            footprint = result_queue.get()

            if footprint.geom is None:
                with open(log_file,'a') as log:
                    print("Error processing {}... {}".format(footprint.filename,footprint.crs),file=log)
                continue

            if footprint.geom == 'kill':
                with open(log_file,'a') as log:
                    print("Rasters complete. Closing shapefile writer.", file=log)
                return 0

            sf.write_footprint(footprint)

            with open(log_file,'a') as log:
                print("Wrote {} footprint ({})".format(footprint.filename,footprint.meta['type']), file=log)

        except:
            with open(log_file,'a') as log:
                print("Error while writing", footprint.filename,file=log)
            traceback.print_exc()


class ShapefileWriter:
    def __init__(self,name,sizelimit=None):
        self.max_size = sizelimit or int(2e9 - 1e8)

        self.crs = CRS.from_epsg('4326')
        self.schema = {'geometry': 'Polygon', 'properties': {'location': 'str:150',
                                                             'type': 'str:30',
                                                             'sensor': 'str:16',
                                                             'date': 'str:19',
                                                             'year': 'int',
                                                             'month': 'int',
                                                             'day': 'int'}}
        self.base_name, _ = os.path.splitext(name)

        self.name = [name]
        self.current_file()
        print("Writing output to ",self.name[-1])

    def make_empty(self):
        if not os.path.isfile(self.name[-1]):
            print("Making shapefile ({})".format(self.name[-1]))
            sf = fiona.open(self.name[-1], 'w', 'ESRI Shapefile', self.schema, crs=self.crs)
            sf.close()

    def current_file(self,force_new=False):
        # if current name doesnt exist, create file
        if not os.path.isfile(self.name[-1]):
            self.make_empty()
            return self.name[-1]
        statinfo = os.stat(self.name[-1])
        if statinfo.st_size > self.max_size or force_new:
            self.name.append("{}({}).shp".format(self.base_name, len(self.name)))
            self.make_empty()
        return self.name[-1]

    def write_footprint(self, footprint):
        file = self.current_file()
        print("writing to {}".format(file))
        with fiona.open(file, 'a') as layer:
            layer.write({'geometry': footprint.geom,
                         'properties': footprint.meta})


def bulk_process_footprints(files, output, cores=None, log_file=None, bb=False):
    crs = CRS.from_epsg('4326')
    log_file = log_file or "footprint_processing_log.txt"
    manager = multiprocessing.Manager()
    result_queue = manager.Queue()
    n_proc = max(2,cores or multiprocessing.cpu_count()-1)
    pool = multiprocessing.Pool(n_proc)

    # put listener to work first
    watcher = pool.apply_async(write_footprint, (output, crs, result_queue, log_file))
    jobs = []

    for file in files:
        job = pool.apply_async(extract_footprint_worker, (file, result_queue, bb))
        jobs.append(job)

    results = []
    failed = []

    for job, file in zip(jobs, files):
        try:
            result = job.get(360),True
            results.append(result)
        except:
            with open(log_file,'a') as log:
                print("{} timed out".format(file),file=log)
            failed.append(file)

    result_queue.put(Footprint(None, 'kill', None, None))
    pool.close()
    watcher.get()

    return {'succeeded': results, 'failed': failed}


if __name__ == "__main__":
    import glob
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-input", help="Input directory containing rasters")
    parser.add_argument("-output", help="Output path for footprint shapefile", default='footprint.shp')
    parser.add_argument("--cores", help="Number of cores to use in parallel processing. Defaults to number avialable - 1")
    parser.add_argument("--log", help="Logging file")
    parser.parse_args()
    args = parser.parse_args()

    band = 0

    cores = int(args.cores)

    # Either search input directory for tif files, read current directory, or read from stdin
    if not args.input:
        if not sys.stdin.isatty():
            files = [file for file in sys.stdin.read().split("\n") if file.endswith('.tif')]
        else:
            files = glob.glob('*.tif')
    else:
        files = glob.glob(os.path.join(args.input,'*.tif'))


    output = args.output
    output_dir,output_shapefile = os.path.split(args.output)
    output_log = 'footprint_log.txt'

    shapefile = args.output
    log_file = os.path.join(output_dir,output_log)

    n_files = len(files)

    start_time = datetime.datetime.now()
    with open(log_file,'w') as log:
        print("Processing {} rasters @ {}".format(len(files),start_time),file=log)

    results = bulk_process_footprints(files, shapefile, cores=cores, log_file=log_file, bb=True)

    with open(log_file,'a') as log:
        print("Finished processing {} (of {}) rasters in {}".format(len(results['succeeded']),
                                                                   n_files,
                                                                   datetime.datetime.now() - start_time), file=log)

