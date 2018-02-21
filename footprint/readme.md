## Footprint

Python script to produce footprint files for collections of rasters (.tif format).
Approximates the functionality of gdaltindex, but with parallel processing and no limit on number of input files.

Usage:

```
usage: python3 footprint.py [-h] [-input INPUT] [-output OUTPUT] [--cores CORES]
                    [--log LOG]

optional arguments:
  -h, --help      show help message and exit
  -input INPUT    Input directory containing rasters
  -output OUTPUT  Output path for footprint shapefile
  --cores CORES   Number of cores to use in parallel processing. Defaults to
                  number avialable - 1
  --log LOG       Logging file
```

A list of rasters can also be passed to the script via stdin.
```
ls ortho*.tif | python footprint.py
```

If rasters are produced by DigitalGlobe the script will attempt to extract additional meta-data including capture date
and sensor.

Output footprints are in EPSG:4326.
