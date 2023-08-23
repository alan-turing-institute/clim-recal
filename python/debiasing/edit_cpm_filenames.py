# Temporary script to rename the CPM data created by Ruth's code to fit with the format
# expected by the debiasing python code in clim-recal.

import glob
import shutil
import os
from pathlib import Path

# input Hads data folder
path = '/Volumes/vmfileshare/ClimateData/Interim/CPM/three.cities'

# output Hads data folder - NOTE: this is a local path, please change to local or Azure path
path_output = './debiasing_test/scenario'
# path_output = '/Volumes/vmfileshare/ClimateData/Interim/CPM/three.cities.greg/'

# create a list of input and output files
files_in = []
files_in.extend([f for f in glob.glob(path + "**/*/*.tif", recursive=True)])
files_out = [f for f in files_in]
files_out = [f.replace("1980_2000", "19800101-19991230") for f in files_out]
files_out = [f.replace("2000_2010", "20000101-20091230") for f in files_out]
files_out = [f.replace("2010_2020", "20100101-20191230") for f in files_out]
files_out = [f.replace("2020_2040", "20200101-20391230") for f in files_out]
files_out = [f.replace("2040_2060", "20400101-20591230") for f in files_out]
files_out = [f.replace("2060_2080", "20600101-20791230") for f in files_out]
files_out = [f.replace(path, path_output) for f in files_out]

# copy - including recursive directory creation
for i, file_in in enumerate(files_in):
    if not os.path.exists(os.path.dirname(files_out[i])):
        path = Path(os.path.dirname(files_out[i]))
        path.mkdir(parents=True)

    shutil.copy(file_in, files_out[i])
