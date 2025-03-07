import pdal
import os
import time
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from uvars import las2tif_dpath, meta_csv_fn


def laz_to_tif(laz_fn, tif_fn, epsg_code, res, window_size=10, nodata=-9999, dtm=True, reclassify=False):
    """
    Converts a LAZ file to a GeoTIFF using PDAL, with options for reprojection, classification filtering,
    and DTM/DSM generation. Skips processing if the output file already exists.

    Parameters:
        laz_fn (str): Path to the input LAZ file.
        tif_fn (str): Path to the output TIFF file.
        epsg_code (int): EPSG code for reprojection.
        res (float): Resolution of the output raster.
        window_size (int): Window size for IDW interpolation (default: 10).
        nodata (int): NoData value for the output raster (default: -9999).
        dtm (bool): If True, generate a Digital Terrain Model (DTM). Otherwise, generate a DSM (default: True).
        reclassify (bool): If True, apply classification filtering and reclassification (default: False).

    Returns:
        None
    """
    try:
        # Check if the output file already exists
        if os.path.exists(tif_fn):
            print(f"Output file '{tif_fn}' already exists. Skipping processing.")
            return

        # Start timing the execution
        start_time = time.time()

        # Initialize the PDAL pipeline with the LAZ reader
        pipeline = pdal.Reader(laz_fn)

        # Handle reclassification if enabled
        if reclassify:
            tif_fn = tif_fn.replace('.tif', '_yrclf.tif')
            pipeline |= pdal.Filter.expression(expression="Classification != 7")  # Exclude noise points
            pipeline |= pdal.Filter.assign(assignment="Classification[:]=0")      # Reclassify all points
            pipeline |= pdal.Filter.reprojection(out_srs=f"EPSG:{epsg_code}")     # Reproject to target CRS
            pipeline |= pdal.Filter.smrf()                                        # Apply SMRF ground filtering
        else:
            tif_fn = tif_fn.replace('.tif', '_nrclf.tif')
            pipeline |= pdal.Filter.reprojection(out_srs=f"EPSG:{epsg_code}")     # Reproject without reclassification

        # Filter points for DTM or DSM generation
        if dtm:
            pipeline |= pdal.Filter.expression(expression="Classification == 2")  # Keep only ground points
        else:
            print("Generating DSM (Digital Surface Model) by including all surface features such as buildings and vegetation.")

        # Write the output to a GeoTIFF file
        pipeline |= pdal.Writer.gdal(
            filename=tif_fn,
            gdalopts="tiled=yes,compress=deflate",
            nodata=nodata,
            output_type="idw",          # Use Inverse Distance Weighting for interpolation
            resolution=res,             # Set the resolution of the output raster
            window_size=window_size     # Set the window size for IDW
        )

        # Execute the pipeline
        pipeline.execute()

        # Calculate and print the execution time
        elapsed_time = time.time() - start_time
        print(f"Processing completed for {tif_fn} in {elapsed_time:.2f} seconds.")

        # Print summary information
        point_count = len(pipeline.arrays[0]) if pipeline.arrays else 0
        print(f"Processed point cloud contains {point_count} points")
        print(f"Output saved to {tif_fn}")

    except Exception as e:
        print(f"An error occurred during processing for {laz_fn}: {e}")


def get_params(df, idx):
    dname = str(df['transect'][idx])
    laz_fn = df['filepath'][idx]
    epsg_code = int(df['epsg'][idx])
    return dname, laz_fn, epsg_code


def process_file(idx, df, res, window_size, nodata, vname, las2tif_dpath, dtm=True, reclassify=True):
    """
    Helper function to process a single file. Used for parallel processing.
    """
    dname, laz_fn, epsg_code = get_params(df, idx)
    outdpath = os.path.join(las2tif_dpath, dname)
    bname = os.path.basename(laz_fn).replace('.laz', '')
    os.makedirs(outdpath, exist_ok=True)
    tif_fn = os.path.join(outdpath, f"{bname}-{vname}{res}m_{window_size}.tif")
    laz_to_tif(laz_fn, tif_fn, epsg_code, res, window_size, nodata, dtm=dtm, reclassify=reclassify)


if __name__ == "__main__":
    ti = time.perf_counter()
    # Configuration parameters
    res = 30  # Resolution of the output raster
    window_size = 10  # Window size for IDW interpolation
    vname = 'DTM'  # Output file name prefix
    nodata = -9999  # NoData value for the output raster
    num_workers = 20  # Number of parallel workers

    # Load metadata CSV
    df = pd.read_csv(meta_csv_fn)

    # Use ProcessPoolExecutor for parallel processing
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for idx in range(len(df)):
            #if idx > 3: break # Limit to first 4 rows for testing purposes
                
            futures.append(executor.submit(process_file, idx, df, res, window_size, nodata, vname, las2tif_dpath, dtm=True, reclassify=True))

        # Wait for all futures to complete and handle results
        for future in as_completed(futures):
            try:
                future.result()  # Raise any exceptions that occurred during processing
            except Exception as e:
                print(f"Error during parallel processing: {e}")
    
    tf = time.perf_counter() - ti 
    print(f"RUN.TIME {tf/60} min(s)")