from glob import glob 
import os 


fpattern = "/media/ljp238/12TBWolf/ARCHIEVE/EBA3ROIsBrazilianAmazonPointCloudTransact/PC2TIF/*/*DTM30m_10_yrclf.tif"
files = glob(fpattern); print(f'{len(files)} files')