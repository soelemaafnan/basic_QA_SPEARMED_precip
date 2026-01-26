#!/bin/bash
#SBATCH -A gfdl_a
#SBATCH -J mean_fut
#SBATCH -o %x_%j.out
#SBATCH -e %x_%j.err
#SBATCH -p analysis
#SBATCH -t 24:00:00
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
ls -ld /data/2/GFDL-LARGE-ENSEMBLES/TFTEST/SPEAR_c192_o1_Scen_SSP585_IC2011_K50/
echo "----------------------"

# --- Execute the Python script ---
echo "Starting Python Dask script..."
python /nbhome/Soelem.Bhuiyan/basic_QA_SPEARMED_precip/src/mean_std_fut.py
echo "Script finished."
