#!/usr/env/python

""" Run over a month of model data and produce a month-long precip file on the trmm grid using area weighted average regridding

 USAGE: 
  $ module load pythonlib/iris
  $ python run_monthly_regrid.py MODEL YEAR MONTH
"""

import sys, os
import iris
from datetime import datetime as dt
from datetime import timedelta as delt
from glob import glob
import collections
import multiprocessing, logging
import time
import signal
from netCDF4 import date2num, num2date

import regrid_barra2trmm as regrid_trmm

iris.FUTURE.netcdf_promote = True
iris.FUTURE.netcdf_no_unlimited = True

MODEL = str(sys.argv[1])
YEAR = int(sys.argv[2])
MONTH = int(sys.argv[3])
MONTHLY_OUTDIR = '/short/du7/nwe548/reanalysis/trmm_analysis/{}/spec'.format(MODEL)
MONTHLY_OUTFL = 'accum_prcp.{}.trmm_grid.{:04d}{:02d}.nc'.format(MODEL,YEAR,MONTH)

OUTDIR = '/short/du7/nwe548/reanalysis/trmm_analysis/{}/spec/{:04d}/{:02d}'.format(MODEL, YEAR, MONTH)
OUTFL_TMPL = 'accum_prcp.{}.trmm_grid.{}.nc'

#num_proc = 16
_iso_format = '%Y%m%dT%H%MZ'

def return_output_filename( basedate, mod=MODEL, out_fn_template = OUTFL_TMPL, out_dir=OUTDIR ):
    fn = out_fn_template.format(mod, basedate.strftime(_iso_format))
    return os.path.join(out_dir, fn)

def init_worker():
        # redirect incomming signals to the workers so that they ignore sigints, only the parent process can manage them
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def fix_time_coords(cube, field, filename):
    #mdata = cube.metadata
    #new_mdata = iris.cube.CubeMetadata(
        #            mdata.standard_name,
        #            mdata.long_name,
        #            mdata.var_name,
        #            mdata.units,
        #            "",
        #            mdata.cell_methods
        #            )
    #cube.metadata = new_mdata
    cube.remove_coord('forecast_reference_time')
    tcoord = cube.coord('time')
    fpcoord = cube.coord('forecast_period')
    # fix time and forecast_period points. Accum/average cell methods apply the time value to be 
    bd = dt.strptime( filename.split('-')[-1].strip('.nc'), _iso_format)
    numbd = date2num( bd, tcoord.units.name)
    # now replace the time dim and forecast_period dim with the fixed ones
    cube.remove_coord('time')
    new_tcoord = iris.coords.DimCoord(numbd + ( tcoord.points - numbd ) * 2 + 3., 
                    standard_name = 'time',
                    long_name  = 'time',
                    units = tcoord.units,
                    coord_system = tcoord.coord_system
                    )
    cube.add_dim_coord(new_tcoord, 0)
    # now replace the forecast_period dim with the fixed one
    cube.remove_coord('forecast_period')
    new_fpcoord = iris.coords.AuxCoord(( tcoord.points - numbd ) * 2 + 3.,
                    standard_name = 'forecast_period',
                    long_name  = 'forecast_period',
                    units = fpcoord.units)
    cube.add_aux_coord(new_fpcoord, data_dims = 0)
    # needs bounds to do area average regridding
    cube.coord('latitude').guess_bounds()
    cube.coord('longitude').guess_bounds()

def save_regridded_cube( cb ):
    # Calculate the basedate as T0 - 4.5 hours and save cube as filename
    bd = num2date(cb.coord('time').points[0], cb.coord('time').units.name) - delt(hours=1,minutes=30)
    iris.save( cb, return_output_filename( bd ) )
    
def test_regridder(cube):
    return cube

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

assert MODEL == 'BARRA_R', "Not yet configured for model {}".format(MODEL)

_iso_format = '%Y%m%dT%H%MZ' # ISO 8601 format
mod_data_fp_template = '/g/data/ma05/{model}/v1/{typeLong}/spec*/accum_prcp/{year:04d}/{month:02d}/accum_prcp-{typeShort}-spec*-{timeDur}-BARRA_R-v1-{date}.nc'

if not os.path.exists(OUTDIR):
    os.makedirs(OUTDIR)

fnDict = collections.defaultdict(list)
for typeLong, typeShort, timeDur in [('forecast','fc', 'PT10M'), \
            ('analysis','an', 'PT0H')]:
    fnDict[typeShort] = glob(mod_data_fp_template.format(
        model=MODEL,
        typeLong=typeLong,
        typeShort=typeShort,
        timeDur=timeDur,
        year=YEAR,
        month=MONTH,
        date="*")
        )
    fnDict[typeShort].sort()
    
assert len(fnDict['an']) == len(fnDict['fc']), "Length mismatch {} != {}".format(len(fnDict['an']), len(fnDict['fc']))
file_list = []
# reorder
[ file_list.extend([a, f]) for a,f in zip(fnDict['an'], fnDict['fc']) ]

# load forecast cubes
fc_cubes = iris.load(fnDict['fc'], callback = fix_time_coords)
# HOURS NEEDED [  1.5,   4.5,   7.5,   7.5,  10.5,  13.5,  13.5,  16.5,  19.5, 19.5,  22.5]
# IDXs NEEDED [8, 26, 44] (common across all 4 cycles)
# use tidxList to define the indices of the hours we need to extract
tidxList = [8,26,44] 
fc_cubes = iris.cube.CubeList([ fcc[tidxList,:,:] for fcc in fc_cubes ] )
fc_total_cubes = len(fc_cubes)


regridder = regrid_trmm.area_weighted_regridder(model = MODEL)

# set the multiprocess logging configuration
logger = multiprocessing.log_to_stderr()
logger.setLevel(multiprocessing.SUBDEBUG)

# define a pool of workers
pool = multiprocessing.Pool(initializer=init_worker)

# launch processes in asyncronous worker pool
#print("TESTING ON SUBSET")
#pool_results = [ (idx, pool.apply_async(test_regridder,(fc_cube,))) for idx,fc_cube in enumerate(fc_cubes)]
pool_results = [ (idx, pool.apply_async(regridder,(fc_cube,))) for idx,fc_cube in enumerate(fc_cubes)]

# get results
##fc_regrid_cubes = [res.get(timeout=20) for res in pool_results]

fc_regrid_cubes = []
try:
    for idx, result in pool_results:
        fc_regrid_cubes.append(result.get())
        print('{}/{} REGRIDDING DONE...'.format(idx, fc_total_cubes))
    # catch interrupt
except KeyboardInterrupt:
    pool.terminate()
    pool.join()
else:
    print("Finished pool regridding normally")
    pool.close()
    pool.join()

fc_regrid_cubes = iris.cube.CubeList(fc_regrid_cubes)
# now concatenate into one cube
try:
    fc_cube = fc_regrid_cubes.concatenate_cube()
    iris.save(fc_cube, os.path.join(MONTHLY_OUTDIR,MONTHLY_OUTFL) ) 
except: 
    # define a pool of workers
    pool = multiprocessing.Pool(initializer=init_worker)
    print('Could not conatenate to one cube')
    pool_write_results = [ (idx,pool.apply_async(save_regridded_cube, (fc,))) for idx, fc in enumerate( fc_regrid_cubes ) ]
    #[ save_regridded_cube(fc) for fc in fc_regrid_cubes  ]
    #sys.exit(0)
    try:
        for idx, result in pool_write_results:
            result.get()
            print('{}/{} SAVING DONE...'.format(idx, fc_total_cubes))
        # catch interrupt
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()
    else:
        print("Quitting normally")
        pool.close()
        pool.join()
    
print('Success.')

