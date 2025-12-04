SPEAR-MED Precipitation Analysis Framework📖 OverviewThis repository contains a scalable, parallelized workflow for validating and analyzing precipitation output from the SPEAR-MED (Seamless System for Prediction and EArth System Research) Large Ensemble climate model.Designed for High-Performance Computing (HPC) environments, this framework utilizes Dask to process Terabytes of NetCDF data across multiple compute nodes. It addresses two primary scientific quality assurance needs:Anomaly Detection ("Rogue Pixels"): Scans high-frequency output (6-hourly) to identify and visualize numerical instabilities or physically implausible precipitation values.Ensemble Statistics: Calculates climatological means, total accumulated precipitation, and inter-member variability across the 30-member ensemble to assess model consistency.🏗️ Project StructureThe repository is organized to separate configuration, source code, and execution logic:spear-precipitation-analysis/
├── configs/               # YAML configuration files for flexible parameter tuning
│   └── config.yaml        # Main configuration (paths, thresholds, plot settings)
├── src/                   # Core Python source code
│   ├── __init__.py
│   ├── rogue_check.py     # Main orchestrator for anomaly detection
│   ├── workers.py         # Dask worker functions (parallel processing logic)
│   └── viz.py             # Visualization utilities (Cartopy/Matplotlib)
├── notebooks/             # Jupyter notebooks for exploratory analysis & violin plots
├── slurm/                 # Slurm job submission scripts
│   └── myjob_hist.sl
├── logs/                  # Execution logs (git-ignored)
├── results/               # Output directory
│   ├── figures/           # Generated alert maps and statistical plots
│   └── tables/            # CSV summaries of ensemble statistics
├── requirements.txt       # Python dependencies
└── README.md              # Project documentation
Features Parallellization: Utilizes dask.delayed and dask.distributed to scale processing linearly with available CPU cores.IO: leverages xarray and netCDF4 for lazy loading, ensuring memory efficiency.Visualization: If a "rogue pixel" (value > threshold) is detected, the pipeline automatically generates a high-resolution map of the specific timestep and location.Logging: Tracking of every processed file, including timing metrics and error handling.Installation & PrerequisitesThis workflow requires a Python environment with the following scientific libraries:# Clone the repository
git clone [https://github.com/soelemaafnan/spear-precipitation-analysis.git](https://github.com/soelemaafnan/spear-precipitation-analysis.git)
cd spear-precipitation-analysis

# Install dependencies (using conda or pip)
pip install -r requirements.txt
Key Dependencies:xarray & netCDF4: Data handlingdask[distributed]: Parallel computingcartopy & matplotlib: Geospatial plottingpyyaml: Configuration management⚙️ ConfigurationAll run-time parameters are managed via configs/config.yaml. You do not need to modify the code to change paths or thresholds.# Example config.yaml
input_directory: "/path/to/SPEAR/data/"
output_directory: "results/figures"
log_file: "logs/processing.log"

variable_name: "pr"       # Variable ID in NetCDF (e.g., 'precip' or 'pr')
threshold: 500.0          # Flag values > 500 kg/m^2/s

plot_settings:
  dpi: 150
  cmap: "Blues"
📊 Usage1. Running the Rogue Pixel CheckerThis script recursively finds all NetCDF files, checks them against the threshold defined in config.yaml, and generates plots for anomalies.Interactive Run:python src/rogue_check.py
HPC (Slurm) Run:sbatch slurm/myjob_hist.sl
2. Calculating Ensemble StatisticsTo calculate the mean and total precipitation for each of the 30 ensemble members:python src/ensemble_stats.py
Output: results/tables/ensemble_member_total_stats.csv3. Visualizing Ensemble VariabilityUse the notebooks in notebooks/ to generate violin plots comparing the Z-scores of the ensemble members.📜 LicenseThis project is licensed under the MIT License - see the LICENSE file for details.📧 ContactSoelem Bhuiyan soelem.bhuiyan (at) princeton.edu
