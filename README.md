# SPEAR-MED Precipitation Analysis

This repository contains tools for validating and analyzing precipitation output from the SPEAR-MED Large Ensemble climate model.

## Features
* **Rogue Pixel Detection:** Parallelized Dask workflow to scan Terabytes of precipitation and temperature NetCDF data for physical anomalies.
* **Ensemble Statistics:** Calculates mean and total precipitation across 30 ensemble members.
* **Visualization:** Automated generation of violin plots and comparisons.

## Project Structure
* `src/`: Main processing scripts (Dask/Python).
* `notebooks/`: Plotting and exploratory analysis.
* `configs/`: YAML configuration for file paths and thresholds.
* `slurm/`: Job submission scripts for HPC.

## Usage
1. Choose the correct config file from the "configs" directory for your purpose. For example, if you would like to look for anomalies in the atmospheric maximum temperature data (t_ref_max), choose qa_tmax.yaml file and edit the required paths, experiment id and threshold values.
2. Call the correct python script and the corresponding .yaml file in the slurm script with correct filepaths. For example, if you would like to look for maximum temperature anomalies, edit the 32nd line of the slurm script like this - python /nbhome/Soelem.Bhuiyan/basic_QA_SPEARMED_precip/src/outlier_checker.py --config /nbhome/Soelem.Bhuiyan/basic_QA_SPEARMED_precip/configs/qa_tmax.yaml
3. Cd to the slurm directory, submit the job: `sbatch myjob_basic.sl`.

## Contact
Soelem Aafnan Bhuiyan
Princeton/NOAA/OAR/GFDL
soelem.bhuiyan (at) princeton.edu
