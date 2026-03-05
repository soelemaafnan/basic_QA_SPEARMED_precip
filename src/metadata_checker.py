import os
import glob
import time
import netCDF4
import numpy as np
import csv
import logging
import yaml
from collections import Counter
import dask
from dask.distributed import Client

# --- PATH SETUP ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'configs', 'config.yaml')

# Load Configuration
try:
    with open(CONFIG_PATH, 'r') as f:
        YAML_CONFIG = yaml.safe_load(f)
except FileNotFoundError:
    print(f"Error: Configuration file not found at {CONFIG_PATH}")
    exit(1)

# Extract variable name from config (defaults to 'pr' if missing)
VAR_NAME = YAML_CONFIG.get('variable_name', 'pr')

# --- CONFIGURATION ---
CONFIG = {
    "input_directory": YAML_CONFIG['input_directory'],
    "output_log": os.path.join(PROJECT_ROOT, "logs", YAML_CONFIG.get('log_file', 'metadata_integrity.log')),
    "output_csv_failures": os.path.join(PROJECT_ROOT, YAML_CONFIG.get('output_directory', 'outputs'), "failed_metadata_files.csv"),
    "search_pattern": f"**/{VAR_NAME}*.nc"
}

# --- THE GOLDEN STANDARD RULES ---
# This dictionary defines exactly what the metadata MUST look like.
EXPECTED_METADATA = {
    "dimensions": {
        "lat": 360,
        "lon": 576,
        "bnds": 2
    },
    "variables": {
        VAR_NAME: {
            "units": "kg m-2 s-1",
            "cell_methods": "area: time: mean",
            "missing_value": 1.e+20,
            "_FillValue": 1.e+20,
            "standard_name": "precipitation_flux"
        },
        "lat": {
            "units": "degrees_north"
        },
        "lon": {
            "units": "degrees_east"
        },
        "time": {
            "calendar": "julian",
            "axis": "T"
            # Note: We don't check exact time units here because the base date 
            # might change slightly per file. We check it dynamically below.
        }
    },
    "globals": {
        "experiment_id": "scenarioSSP5-85",
        "frequency": "6hr",
        "source_id": "GFDL-SPEAR-MED",
        "activity_id": "SPEAR"
    }
}

@dask.delayed
def check_file_integrity(filepath: str) -> dict:
    """
    Worker function to open a single NetCDF file, read ONLY its metadata,
    and verify it against the Golden Standard rules.
    """
    errors = []
    tracking_id = None
    basename = os.path.basename(filepath)
    
    try:
        # Open in 'r' mode. netCDF4 reads metadata instantly without loading arrays.
        with netCDF4.Dataset(filepath, 'r') as ds:
            
            # 1. Check Global Attributes
            for key, expected_val in EXPECTED_METADATA["globals"].items():
                if not hasattr(ds, key):
                    errors.append(f"Missing Global Attr: '{key}'")
                elif getattr(ds, key) != expected_val:
                    errors.append(f"Global '{key}' is '{getattr(ds, key)}', expected '{expected_val}'")

            # Extract tracking_id for uniqueness check later
            tracking_id = getattr(ds, 'tracking_id', "MISSING_ID")
            if tracking_id == "MISSING_ID":
                errors.append("Missing Global Attr: 'tracking_id'")

            # 2. Check Dimensions
            for dim, expected_size in EXPECTED_METADATA["dimensions"].items():
                if dim not in ds.dimensions:
                    errors.append(f"Missing Dimension: '{dim}'")
                elif ds.dimensions[dim].size != expected_size:
                    errors.append(f"Dim '{dim}' size is {ds.dimensions[dim].size}, expected {expected_size}")

            # 3. Check Variables
            for var_name, expected_attrs in EXPECTED_METADATA["variables"].items():
                if var_name not in ds.variables:
                    errors.append(f"Missing Variable: '{var_name}'")
                    continue
                
                var = ds.variables[var_name]
                for attr, expected_val in expected_attrs.items():
                    if not hasattr(var, attr):
                        errors.append(f"Var '{var_name}' missing attribute '{attr}'")
                    else:
                        actual_val = getattr(var, attr)
                        
                        # Special handling for floating point values (like missing_value)
                        if isinstance(expected_val, float):
                            if not np.isclose(actual_val, expected_val):
                                errors.append(f"Var '{var_name}:{attr}' is {actual_val}, expected {expected_val}")
                        else:
                            if actual_val != expected_val:
                                errors.append(f"Var '{var_name}:{attr}' is '{actual_val}', expected '{expected_val}'")

            # 4. Dynamic Checks
            # Check if time units start with "days since"
            if "time" in ds.variables and hasattr(ds.variables["time"], "units"):
                if not getattr(ds.variables["time"], "units").startswith("days since"):
                    errors.append(f"Time units do not start with 'days since'. Found: {ds.variables['time'].units}")

            # Verify Realization Index matches folder name (e.g., pp_ens_05 -> realization_index = 5)
            import re
            match = re.search(r'pp_ens_(\d+)', filepath)
            if match and hasattr(ds, 'realization_index'):
                expected_realization = int(match.group(1))
                actual_realization = int(getattr(ds, 'realization_index'))
                if actual_realization != expected_realization:
                    errors.append(f"Folder is ens_{expected_realization:02d} but file realization_index is {actual_realization}")

    except Exception as e:
        errors.append(f"FILE CORRUPTION or READ ERROR: {e}")

    return {
        "filepath": filepath,
        "basename": basename,
        "status": "PASS" if len(errors) == 0 else "FAIL",
        "errors": " | ".join(errors),
        "tracking_id": tracking_id
    }

def main(client: Client):
    # Setup logging directory
    os.makedirs(os.path.dirname(CONFIG["output_log"]), exist_ok=True)
    os.makedirs(os.path.dirname(CONFIG["output_csv_failures"]), exist_ok=True)
    
    logging.basicConfig(
        filename=CONFIG["output_log"],
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w'
    )

    start_time = time.time()
    
    logging.info(f"Scanning directory: {CONFIG['input_directory']}")
    search_path = os.path.join(CONFIG["input_directory"], CONFIG["search_pattern"])
    all_files = glob.glob(search_path, recursive=True)
    
    if not all_files:
        logging.error("No NetCDF files found! Check your path.")
        return
        
    logging.info(f"Found {len(all_files)} files. Starting integrity checks...")
    
    # Map tasks to Dask
    futures = client.map(check_file_integrity, all_files)
    logging.info("Waiting for Dask workers to complete...")
    results = client.gather(futures)
    
    # --- ANALYSIS & REPORTING ---
    failed_files = [r for r in results if r["status"] == "FAIL"]
    passed_count = len(results) - len(failed_files)
    
    # Check for duplicate Tracking IDs
    tracking_ids = [r["tracking_id"] for r in results if r["tracking_id"] != "MISSING_ID"]
    id_counts = Counter(tracking_ids)
    duplicates = {tid: count for tid, count in id_counts.items() if count > 1}
    
    logging.info("="*40)
    logging.info("INTEGRITY CHECK COMPLETE")
    logging.info("="*40)
    logging.info(f"Total Files Scanned : {len(results)}")
    logging.info(f"Passed              : {passed_count}")
    logging.info(f"Failed              : {len(failed_files)}")
    logging.info(f"Duplicate UUIDs     : {len(duplicates)}")
    logging.info(f"Time Elapsed        : {time.time() - start_time:.2f} seconds")
    
    # Save Failures to CSV
    if failed_files:
        with open(CONFIG["output_csv_failures"], 'w', newline='') as f:
            # Match the keys returned by the worker dictionary
            writer = csv.DictWriter(f, fieldnames=["filepath", "basename", "status", "errors", "tracking_id"])
            writer.writeheader()
            for row in failed_files:
                writer.writerow(row)
        logging.warning(f"Wrote failure details to: {CONFIG['output_csv_failures']}")
    else:
        logging.info("All files passed standard metadata checks!")
        
    if duplicates:
        logging.warning("Duplicate tracking_ids detected! This means files were accidentally copied/renamed.")
        logging.warning("DUPLICATE TRACKING IDs FOUND:")
        for tid, count in duplicates.items():
            logging.warning(f"ID: {tid} appeared {count} times")

if __name__ == "__main__":
    # Start Dask client
    client = Client()
    # We leave these two prints so you can quickly check the Slurm .out file to see where logs are going
    print(f"Dask Dashboard: {client.dashboard_link}")
    print(f"Background processing started. Check logs at: {CONFIG['output_log']}")
    
    main(client)
    
    client.close()