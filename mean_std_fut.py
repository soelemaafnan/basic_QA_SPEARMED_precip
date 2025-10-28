import os
import glob
import csv
import time
import dask
import xarray as xr
from dask.distributed import Client, progress

CONFIG = {
    "base_directory": "/data/2/GFDL-LARGE-ENSEMBLES/TFTEST/SPEAR_c192_o1_Scen_SSP585_IC2011_K50",
    "output_csv": "ensemble_member_stats_fut.csv",
    "variable_name": "precip",
    "search_pattern": "pp_ens_*",
    "file_pattern": "**/*precip.nc"
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
            "mean": "N/A",
            "std_dev": "N/A",
            "error": "No files found"
        }

    try:
        with xr.open_mfdataset(file_list, parallel=True, chunks='auto', engine='netcdf4') as ds:
            
            var_data = ds[CONFIG['variable_name']]
            
            mean_val_lazy = var_data.mean()
            std_val_lazy = var_data.std()
            
            print(f"  [{member_name}] Calculating mean and std...")
            mean_val, std_val = dask.compute(mean_val_lazy, std_val_lazy)

        duration = time.time() - start_time
        print(f"COMPLETED: {member_name} in {duration:.2f} seconds.")
        
        return {
            "member": member_name,
            "mean": float(mean_val),
            "std_dev": float(std_val),
            "error": None
        }

    except Exception as e:
        duration = time.time() - start_time
        print(f"ERROR: {member_name} failed after {duration:.2f}s. Reason: {e}")
        return {
            "member": member_name,
            "mean": "N/A",
            "std_dev": "N/A",
            "error": str(e)
        }

def main(client: Client):
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
    print(f"Dask client started. Dashboard link (if accessible): {client.dashboard_link}")
    
    main(client)
    
    client.close()