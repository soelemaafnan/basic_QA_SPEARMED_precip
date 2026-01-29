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
    # Fallback for running standalone in src/
    CONFIG_PATH = os.path.join(SCRIPT_DIR, 'config.yaml')

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"Configuration file not found at: {CONFIG_PATH}")

with open(CONFIG_PATH, 'r') as f:
    YAML_CONFIG = yaml.safe_load(f)

# --- VALIDATE CONFIGURATION ---
# Ensure essential keys exist in the YAML. We do not want hardcoded defaults for these.
required_keys = ['input_directory', 'output_directory', 'variable_name']
for key in required_keys:
    if key not in YAML_CONFIG:
        raise KeyError(f"Missing required configuration key: '{key}' in {CONFIG_PATH}")

# Construct output path
output_dir = YAML_CONFIG['output_directory']
if not os.path.isabs(output_dir):
    output_dir = os.path.join(PROJECT_ROOT, output_dir)

# Get the filename from config, default to 'ensemble_stats.csv' if not present
output_filename = YAML_CONFIG.get('output_filename', 'ensemble_member_mean_stats.csv')

# --- INTERNAL CONFIG MAPPING ---
# This dictionary maps the YAML keys to the specific keys used by the worker function.
CONFIG = {
    "base_directory": YAML_CONFIG['input_directory'],
    "output_csv": os.path.join(output_dir, output_filename),
    "variable_name": YAML_CONFIG['variable_name'],
    # Optional patterns: Use YAML if provided, otherwise default to standard structure
    "search_pattern": YAML_CONFIG.get('member_search_pattern', "pp_ens_*"),
    "file_pattern": YAML_CONFIG.get('file_search_pattern', "**/*.nc") 
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
            "mean_precip_mm": "N/A",
            "std_dev_mm": "N/A",
            "error": "No files found"
        }

    try:
        # FIX APPLIED:
        # 1. compat='override': Fixes "conflicting values for variable" error.
        # 2. coords='minimal': Optimization to prevent checking all coordinates.
        with xr.open_mfdataset(
            file_list, 
            parallel=True, 
            chunks={'time': 240}, 
            decode_timedelta=False, 
            engine='netcdf4',
            compat='override',
            coords='minimal' 
        ) as ds:
            
            if CONFIG['variable_name'] not in ds:
                raise ValueError(f"Variable '{CONFIG['variable_name']}' not found")

            var_data = ds[CONFIG['variable_name']]
            
            # Convert rate (kg/m2/s) to mm/day
            # 1 day = 24 * 60 * 60 = 86400 seconds
            precip_rate_mm_day = var_data * 86400
            
            # Lazily define calculations
            # Calculates the mean daily rate across all timesteps/pixels
            mean_precip_lazy = precip_rate_mm_day.mean()
            std_dev_lazy = precip_rate_mm_day.std()
            
            print(f"  [{member_name}] Calculating mean precip (mm/day) and std dev...")
            mean_val, std_val = dask.compute(mean_precip_lazy, std_dev_lazy)

        duration = time.time() - start_time
        print(f"COMPLETED: {member_name} in {duration:.2f} seconds.")
        
        return {
            "member": member_name,
            "mean_precip_mm_day": float(mean_val), # Updated key for clarity
            "std_dev_mm_day": float(std_val),      # Updated key for clarity
            "error": None
        }

    except Exception as e:
        duration = time.time() - start_time
        print(f"ERROR: {member_name} failed after {duration:.2f}s. Reason: {e}")
        return {
            "member": member_name,
            "mean_precip_mm_day": "N/A",
            "std_dev_mm_day": "N/A",
            "error": str(e)
        }

def main(client: Client):
    # Ensure output directory exists
    os.makedirs(os.path.dirname(CONFIG['output_csv']), exist_ok=True)

    print(f"Reading data from: {CONFIG['base_directory']}")
    print(f"Searching for members matching: {CONFIG['search_pattern']}")

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