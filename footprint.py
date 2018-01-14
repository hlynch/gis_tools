import rasterio.features
import rasterio.warp
import fiona
import multiprocessing
from collections import namedtuple
from rasterio.crs import CRS
from rasterio.warp import transform_bounds
import traceback
from functools import wraps
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
import sys
Footprint = namedtuple('Footprint',('crs','geom','filename'))
import os


class BoundingBox:
    def __init__(self,bounds,crs):
        bbox = transform_bounds(crs,
                                {'init': 'epsg:4326'}, *bounds)
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




def extract_footprint(tiff):
    with rasterio.open(tiff) as input_raster:
        # Read the dataset's valid data mask as a ndarray.
        mask = input_raster.read_masks(1)
        transform = input_raster.transform
        crs = input_raster.crs.copy()
        bounds = input_raster.bounds
    print("extracting geometry for {}".format(tiff))
    geom = rasterio.features.shapes(mask, mask=mask, transform=transform)
    geoms = []
    try:
        for i, (g, val) in enumerate(geom):
            g = rasterio.warp.transform_geom(
                input_raster.crs, 'EPSG:4326', g,
                antimeridian_cutting=True)
            geoms.append(g)
        print("geometry extracted for {}".format(tiff))
        return Footprint(crs,geoms[0],tiff)
    except Exception as e:
        print("Error:",e)
        print("Trying bounding box")
        try:
            geom = BoundingBox(bounds,crs).to_geometry()
            return Footprint(crs,geom,tiff)
        except Exception as e:
            print(e)

def extract_footprint_worker(tiff, result_queue,timeout=240):
    p = ThreadPool(1)
    print('starting a worker ({}) with timeout {}'.format(tiff,timeout))
    start = datetime.datetime.now()
    sys.stdout.flush()
    res = p.apply_async(extract_footprint, (tiff,))
    try:
        footprint = res.get(timeout)  # Wait timeout seconds for func to complete.
        print("finished processing {} in {}".format(tiff,datetime.datetime.now()-start))
        result_queue.put(footprint)
        print(result_queue)
        return tiff
    except multiprocessing.TimeoutError:
        print("Aborting due to timeout")
        p.terminate()
        footprint_error = Footprint('Timeout Error',None,tiff)
        result_queue.put(footprint_error)
        print(result_queue.qsize())
        return None


def write_footprint(shapefile,crs,result_queue,log_file):
    '''
    listens for messages on the results queue, writes shapefile.
    '''
    print("listener started")
    schema = {'geometry': 'Polygon', 'properties': {'location': 'str:150'}}
    with fiona.open(shapefile, 'w', 'ESRI Shapefile', schema, crs=crs) as layer:
        while True:
            try:
                footprint = result_queue.get()

                if footprint.geom is None:
                    print("Error processing {}... {}".format(footprint.filename,footprint.crs))
                    continue

                if footprint.geom == 'kill':
                    print("Finished Processing")
                    return 0

                if footprint.crs != crs:
                    pass


                layer.write({'geometry': footprint.geom, 'properties': {'location': footprint.filename}})

                with open(log_file,'a') as log:
                    print("Wrote {} footprint".format(footprint.filename), file=log)

            except:
                traceback.print_exc()
                print(footprint.geom)


def write_footprint_feature(shapefile, crs, geometry, filename):

    schema = {'geometry': 'Polygon', 'properties': {'location': 'str:150'}}

    with fiona.open(shapefile, 'a', 'ESRI Shapefile', schema, crs=crs) as layer:
        layer.write({'geometry': geometry, 'properties': {'location': filename}})

    return True


if __name__ == "__main__":

    import argparse
    import datetime
    import glob

    band = 0
    # directory = '/gpfs/projects/LynchGroup'
    directory = 'tests/raster'
    # output = '/gpfs/projects/LynchGroup/GIS_tools/scratch/footprint.shp'
    output = 'footprint.shp'
    # log_file = '/gpfs/projects/LynchGroup/GIS_tools/footprint_log.txt'
    log_file = 'footprint_log.txt'
    files = glob.glob("{}/*.tif".format(directory))
    # files = files[:3]
    # print(files)
    start_time = datetime.datetime.now()
    with open(log_file,'w') as log:
        print("starting processing {} rasters @ {}".format(len(files),start_time),file=log)
    crs = CRS.from_epsg('3031')
    manager = multiprocessing.Manager()
    result_queue = manager.Queue()
    n_proc = multiprocessing.cpu_count()-1
    with open(log_file,'a') as log:
        print("setting up {} workers".format(n_proc),file=log)
    pool = multiprocessing.Pool(n_proc)

    #put listener to work first
    watcher = pool.apply_async(write_footprint, (output,crs,result_queue,log_file))
    jobs = []
    # for file in [files[0]]:

    for file in files:
        job = pool.apply_async(extract_footprint_worker,(file, result_queue))
        jobs.append(job)
    results = []
    failed = []

    for job,file in zip(jobs,files):
        print("Getting result of {}".format(file))
        try:
            results.append(job.get(360))
        except:
            print("{} timed out".format(file))
            failed.append(file)

    print("processed: ",results)
    print("failed: ",failed)
    print("finished {} out of {} jobs".format(len(results),len(files)))
    #now we are done, kill the listener
    result_queue.put(Footprint(None,'kill',None))
    print("Kill signal sent")
    # watcher.get()
    pool.close()

    with open(log_file,'a') as log:
        print("finished processing {} rasters @ {}".format(len(files),datetime.datetime.now()),file=log)
