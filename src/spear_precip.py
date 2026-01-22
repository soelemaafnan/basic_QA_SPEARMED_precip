"""This script checks for rogue pixels in SPEAR-MED precipitation
outputs using Dask for parallel processing. It recursively finds all
'.nc' files, processes each in parallel, and logs the results.
This version prints real-time updates to the terminal as files are completed."""

# Import modules
import os
import glob
import logging
import time
import netCDF4
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import yaml
import dask
from dask.distributed import Client, progress
from datetime import timedelta

# ... imports ...

# REVISED PATH LOGIC
# Get the absolute path of the 'src' directory
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
# Get the project root (one level up from src)
PROJECT_ROOT = os.path.dirname(SRC_DIR)
# Path to config
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'configs', 'config.yaml')

with open(CONFIG_PATH, 'r') as f:
    CONFIG = yaml.safe_load(f)

# Update output paths to be relative to project root, not src
CONFIG['output_directory'] = os.path.join(PROJECT_ROOT, CONFIG['output_directory'])
CONFIG['log_file'] = os.path.join(PROJECT_ROOT, 'logs', 'output.log')


# The plotting function remains the same
def create_alert_plot(data: np.ndarray, lons: np.ndarray, lats: np.ndarray, date: object, filepath: str, t_index: int) -> None:
    """Generates and saves a plot for a specific timestep when a high value is detected."""
    try:
        # Note: 'pr' units are kg m-2 s-1. 
        # Multiplied by 21600s (6 hours) = total mm per 6hr timestep.
        precip_total_mm = data * 21600
        
        fig = plt.figure(figsize=CONFIG['plot_settings']["figure_size"])
        ax = plt.axes(projection=ccrs.Robinson())
        ax.set_global()
        mesh = ax.pcolormesh(
            lons, lats, precip_total_mm, transform=ccrs.PlateCarree(),
            cmap=CONFIG['plot_settings']["cmap"]
        )
        ax.coastlines()
        ax.gridlines(draw_labels=False)
        cbar = plt.colorbar(mesh, orientation='vertical', pad=0.02, aspect=30, shrink=0.8)
        cbar.set_label('Precipitation (mm/6-hr)')
        
        start_time = date
        end_time = start_time + timedelta(hours=6)
        basename = os.path.basename(filepath)
        title = (
            f"High Precipitation Found\n"
            f"Source: {basename} ({start_time.strftime('%Y-%m-%d %H:%M')}-{end_time.strftime('%H:%M')})"
        )
        ax.set_title(title, pad=20)
        
        sanitized_path = basename.replace('.nc', '')
        plot_filename = f"ALERT_{sanitized_path}_timestep_{t_index:05d}.png"
        save_path = os.path.join(CONFIG["output_directory"], plot_filename)
        
        plt.savefig(save_path, dpi=CONFIG['plot_settings']["dpi"], bbox_inches='tight')
        
    except Exception as e:
        print(f"Error plotting {filepath} at timestep {t_index}: {e}")
    finally:
        plt.close(fig)

# "Worker" function decorated with @dask.delayed
@dask.delayed
def process_single_file(filepath: str) -> str:
    """
    Processes a single NetCDF file, prints real-time status, and returns a log message.
    """
    start_time = time.time()
    high_value_found_in_file = False
    basename = os.path.basename(filepath)
    
    try:
        with netCDF4.Dataset(filepath, 'r') as ds:
            # Check if variable exists before accessing
            if CONFIG["variable_name"] not in ds.variables:
                return f"SKIPPED: {basename} (Variable '{CONFIG['variable_name']}' not found)"

            precip_var = ds.variables[CONFIG["variable_name"]]
            lats = ds.variables[CONFIG["lat_dim"]][:]
            lons = ds.variables[CONFIG["lon_dim"]][:]
            time_var = ds.variables[CONFIG["time_dim"]]
            num_timesteps = len(time_var)

            for t_index in range(num_timesteps):
                data_slice = precip_var[t_index, :, :]
                total_precip = data_slice * 21600
                
                # Check for threshold. netCDF4 handles FillValues automatically (masked array)
                if np.any(total_precip > CONFIG["threshold"]):
                    high_value_found_in_file = True
                    date = netCDF4.num2date(time_var[t_index], time_var.units, getattr(time_var, 'calendar', 'standard'))
                    
                    # Print alert immediately to terminal
                    print(f"\nALERT! High value in {basename} at step {t_index}. Plotting...")
                    create_alert_plot(data_slice, lons, lats, date, filepath, t_index)

        end_time = time.time()
        duration = end_time - start_time
        
        print(f"COMPLETED: {basename} in {duration:.2f} seconds.")

        if high_value_found_in_file:
            return f"{filepath} processed in {duration:.2f}s, values greater than {CONFIG['threshold']} kg/m2/s found!"
        else:
            return f"{filepath} processed in {duration:.2f}s, no value greater than {CONFIG['threshold']} kg/m2/s found"

    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"ERROR: Failed to process {basename} after {duration:.2f}s.")
        return f"ERROR processing {filepath} after {duration:.2f}s. Reason: {e}"

# Main orchestrator function
def main() -> None:
    """Main function to find files and orchestrate the parallel analysis."""
    os.makedirs(CONFIG["output_directory"], exist_ok=True)
    
    # REVISED: Updated search pattern to "*.nc" to catch "pr_*.nc" files
    search_pattern = os.path.join(CONFIG["input_directory"], '**', '*.nc')
    file_list = glob.glob(search_pattern, recursive=True)
    
    if not file_list:
        print(f"No '*.nc' files found in '{CONFIG['input_directory']}' or its subdirectories. Exiting.")
        return
        
    print(f"Found {len(file_list)} files to process.")
    
    tasks = [process_single_file(fp) for fp in file_list]
    
    print("Starting Dask client and processing files in parallel...")
    results = dask.compute(*tasks)
    progress(results)
    
    logging.basicConfig(
        filename=CONFIG["log_file"],
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='w'
    )
    
    print(f"\nAll processing complete. Writing results to '{CONFIG['log_file']}'...")
    for log_message in results:
        logging.info(log_message)

    print("Done.")

if __name__ == "__main__":
    client = Client()
    print(f"Dask client started. Dashboard link (if accessible): {client.dashboard_link}")
    
    main()
    
    client.close()