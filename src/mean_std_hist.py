import os
import glob
import csv
import time
import dask
import xarray as xr
import yaml
from dask.distributed import Client, progress

# --- LOAD CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'configs', 'config.yaml')

if not os.path.exists(CONFIG_PATH):
    # Fallback for running standalone
    CONFIG_PATH = os.path.join(SCRIPT_DIR, 'config.yaml')

try:
    with open(CONFIG_PATH, 'r') as f:
        YAML_CONFIG = yaml.safe_load(f)
except FileNotFoundError:
    print("Warning: config.yaml not found. Using defaults.")
    YAML_CONFIG = {}

# Construct internal CONFIG
output_dir = YAML_CONFIG.get('output_directory', 'outputs')
if not os.path.isabs(output_dir):
    output_dir = os.path.join(PROJECT_ROOT, output_dir)

CONFIG = {
    "base_directory": YAML_CONFIG.get('input_directory', "/data/2/GFDL-LARGE-ENSEMBLES/TFTEST/SPEAR_c192_o1_Hist_AllForc_IC1921_K50"),
    "output_csv": os.path.join(output_dir, "ensemble_member_total_stats.csv"),
    "variable_name": YAML_CONFIG.get('variable_name', 'precip'),
    "search_pattern": "pp_ens_*",
    "file_pattern": "**/*.nc" 
}

def calculate_member_stats(member_path: str) -> dict:
    start_time = time.time()
    member_name = os.path.basename(member_path)
    print(f"Processing member: {member_name}...")
    
    search_path = os.path.join(member_path, CONFIG['file_pattern'])
    file_list = glob.glob(search_path, recursive=True)
    
    if not file_list:
        print(f"ERROR: No files found for {member_name}")
        return {
            "member": member_name,
            "total_precip_mm": "N/A",
            "std_dev_mm": "N/A",
            "error": "No files found"
        }

    try:
        # FIX APPLIED HERE:
        # 1. chunks={'time': 240}: Replaced 'auto' with fixed size to avoid object-dtype error.
        # 2. decode_timedelta=False: Ignores the problematic 'average_DT' variable.
        with xr.open_mfdataset(
            file_list, 
            parallel=True, 
            chunks={'time': 240}, 
            decode_timedelta=False, 
            engine='netcdf4'
        ) as ds:
            
            if CONFIG['variable_name'] not in ds:
                raise ValueError(f"Variable '{CONFIG['variable_name']}' not found")

            var_data = ds[CONFIG['variable_name']]
            
            # Convert rate (kg/m2/s) to amount (kg/m2, or mm) per 6-hr timestep
            precip_amount_per_step = var_data * 21600
            
            # Lazily define calculations
            total_precip_lazy = precip_amount_per_step.sum()
            std_dev_lazy = precip_amount_per_step.std()
            
            print(f"  [{member_name}] Calculating total precip and std dev...")
            total_val, std_val = dask.compute(total_precip_lazy, std_dev_lazy)

        duration = time.time() - start_time
        print(f"COMPLETED: {member_name} in {duration:.2f} seconds.")
        
        return {
            "member": member_name,
            "total_precip_mm": float(total_val),
            "std_dev_mm": float(std_val),
            "error": None
        }

    except Exception as e:
        duration = time.time() - start_time
        print(f"ERROR: {member_name} failed after {duration:.2f}s. Reason: {e}")
        return {
            "member": member_name,
            "total_precip_mm": "N/A",
            "std_dev_mm": "N/A",
            "error": str(e)
        }

def main(client: Client):
    # Ensure output directory exists
    os.makedirs(os.path.dirname(CONFIG['output_csv']), exist_ok=True)

    search_path = os.path.join(CONFIG['base_directory'], CONFIG['search_pattern'])
    member_paths = sorted(glob.glob(search_path))
    
    if not member_paths:
        print(f"Error: No member directories found at {search_path}")
        return

    print(f"Found {len(member_paths)} ensemble members to process.")
    
    futures = client.map(calculate_member_stats, member_paths)
    
    print("Waiting for all members to complete...")
    results = client.gather(futures)
    
    print(f"\nAll calculations complete. Writing results to {CONFIG['output_csv']}...")
    
    valid_results = [r for r in results if r is not None]
    
    if not valid_results:
        print("No valid results to write.")
        return

    fieldnames = valid_results[0].keys()
    with open(CONFIG['output_csv'], 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(valid_results)
        
    print("Done.")

if __name__ == "__main__":
    client = Client()
    print(f"Dask client started. Dashboard link: {client.dashboard_link}")
    main(client)
    client.close()