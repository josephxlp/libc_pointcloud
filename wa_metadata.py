import geopandas as gpd
import pandas as pd
import os
from glob import glob
from shapely.geometry import Point  # Ensure we can create geometries if needed
from uvars import dpath, gpkg_fn


def find_closest_match_datafile(datafile, basenames):
    """Finds the closest matching file from a list of basenames (full paths)."""
    base_datafile = os.path.basename(datafile).split('_dn_')[0]  # Extract filename base
    for fullpath in basenames:
        basename = os.path.basename(fullpath)  # Extract filename from full path
        if basename.startswith(base_datafile):
            return fullpath  # Return full path, not just the basename
    return None


# Get all laz files in subdirectories (full paths)
lazfiles = glob(f"{dpath}/*/*.laz")
print(f"Found {len(lazfiles)} laz files.")

basenames = lazfiles  # Keeping full paths to match correctly

# Read the original GeoPackage
gdf = gpd.read_file(gpkg_fn)

meta = gdf.copy()
print(f"Loaded {gpkg_fn}: {len(meta)} records.")
print("Meta DataFrame types:\n", meta.dtypes)

# Ensure it contains a geometry column
if 'geometry' not in meta.columns or meta.geometry.isnull().all():
    print("Warning: No valid geometry found! Adding dummy geometry.")
    meta["geometry"] = [Point(0, 0)] * len(meta)  # Placeholder geometry

# Ensure the GeoDataFrame has a CRS
if meta.crs is None:
    print("Warning: No CRS found. Setting default to EPSG:4326")
    meta.set_crs(epsg=4326, inplace=True)

# Assign full file paths based on matching logic
meta['filepath'] = meta['datafile'].apply(lambda x: find_closest_match_datafile(x, basenames))
print("Sample filepath assignments:\n", meta[['datafile', 'filepath']].head())

# Filter out rows where no match was found
meta = meta[meta['filepath'].notnull()]
print(f"Filtered meta: {len(meta)} records remaining.")
metacols = ['id', 'transect', 'datafile', 'epsg', 'random','geometry','filepath']
meta = meta[metacols]

# Define output filenames
gpkg_fn1 = gpkg_fn.replace('.gpkg', '_locpaths.gpkg')  # Original CRS
gpkg_fn2 = gpkg_fn.replace('.gpkg', '_locpaths_epsg4326.gpkg')  # EPSG:4326 version
csv_fn = gpkg_fn.replace('.gpkg', '_locpaths.csv')  # CSV version

# Save the modified GeoPackage (Original CRS)
print(f"Saving to {gpkg_fn1}...")
meta.to_file(gpkg_fn1, driver="GPKG", overwrite=True)
print("Saved successfully.")

# Save the EPSG:4326 version
meta_4326 = meta.to_crs(epsg=4326)  # Convert CRS
print(f"Saving EPSG:4326 version to {gpkg_fn2}...")
meta_4326.to_file(gpkg_fn2, driver="GPKG", overwrite=True)
print("EPSG:4326 file saved successfully.")

# Save to CSV (without geometry)
df = meta.drop(columns=['geometry'])  # Remove geometry before saving CSV
print(f"Saving CSV version to {csv_fn}...")
df.to_csv(csv_fn, index=False)
print("CSV file saved successfully.")
