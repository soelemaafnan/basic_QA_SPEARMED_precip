# SPEAR-MED Precipitation Analysis

This repository contains tools for validating and analyzing precipitation output from the SPEAR-MED Large Ensemble climate model.

## Features
* **Rogue Pixel Detection:** Parallelized Dask workflow to scan Terabytes of NetCDF data for physical anomalies.
* **Ensemble Statistics:** Calculates mean and total precipitation across 30 ensemble members.
* **Visualization:** automated generation of violin plots and comparisons.

## Project Structure
* `src/`: Main processing scripts (Dask/Python).
* `notebooks/`: Plotting and exploratory analysis.
* `configs/`: YAML configuration for file paths and thresholds.
* `slurm/`: Job submission scripts for HPC.

## Usage
1. Update `configs/config.yaml` with your data paths.
2. Submit the job: `sbatch slurm/myjob_hist.sl`

## Contact
Soelem Aafnan Bhuiyan
Princeton/NOAA/OAR/GFDL
soelem.bhuiyan (at) princeton.edu