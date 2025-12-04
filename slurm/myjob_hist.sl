#!/bin/bash
#SBATCH -A gfdl_a
#SBATCH -J cmor
#SBATCH -o %x_%j.out
#SBATCH -e %x_%j.err
#SBATCH -p analysis
#SBATCH -t 12:00:00
#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mail-user=soelem.bhuiyan@noaa.gov
#SBATCH --mail-type=BEGIN,END,FAIL

# --- Ensure the script runs from the submission directory ---
cd $SLURM_SUBMIT_DIR

# --- Environment Setup ---
module purge
module load conda
conda activate gfdl

# --- Pre-run Debugging ---
echo "--- JOB ENVIRONMENT ---"
echo "Job is running in directory: $(pwd)"
echo "Python executable: $(which python)"
echo "Checking visibility of data directory:"
ls -ld /work/jlz/CMIP6_output/hist_ens_30/CMIP6_output/CMIP6/SPEAR/NOAA-GFDL/GFDL-SPEAR-MED/historical/r30i1p1f1/6hr/pr/gr3/v20251126/
echo "----------------------"

# --- Execute the Python script ---
echo "Starting Python Dask script..."
python spear_precip.py
echo "Script finished."
