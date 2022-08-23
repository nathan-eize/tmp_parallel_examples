# tmp_parallel_examples

Various parallel code example scripts used in the BARRA project

## `run_monthly_regrid.py`
A parallel regridding python script using `iris` and `multiprocessing`. Regrids accumulated rainfall data from BARRA to TRMM observations grid for FSS analaysis. Regrids hundreds of netcdf files from a 12km resolution lat/lon grid with 10 min frequency fields, to 0.25 degrees lat/lon with 90 min frequency fields. 

## `download_pp_ma05_leftovers.sh`
A bash script for parallel downloading from `mdss` using `gnu parallel`. It turns out this isn't recommended because `mdss` has it's own task manager to optimise manual tape retreivals. Best to ask for big chunks of data files at once in serial and let the `mdss` figure the rest out. 

## `download_parallel.sh`
A smarter version of the above using background processes as parallel implementation. Downloads BARRA data from `mdss`. It spins up a `$proc_max` number of parallel processes and uses `ps` to count the number running. If one process finishes it will spin up more background processes up to the max number, until all are done. 
