
# Analysis pipeline guidance

This is a detailed guide to our analysis pipeline.
*see also this [flowchart viz](https://github.com/alan-turing-institute/clim-recal/blob/documentation/docs/pipeline.md) of the pipeline*

**Contents:**
* [Prerequisites](#prerequisites)
    * [Setting up your R environment](#setting-up-your-r-environment)
    * [Setting up your python environment](#setting-up-your-python-environment)
* [Downloading the data](#downloading-the-data)
* [Reprojecting the data](#reprojecting-the-data)
* [Resampling the data](#resampling-the-data)
* [Preparing the bias correction and assessment](#preparing-the-bias-correction-and-assessment)
* [Applying the bias correction](#applying-the-bias-correction)


### Prerequisites

For our bias correction methods, we tap into dedicated packages in both Python and R ecosystems. The integration of these languages allows us to utilize the cutting-edge functionalities implemented in each. Given this dual-language nature of our analysis pipeline, we also provide preprocessing scripts written in both Python and R. To facilitate a seamless experience, users are required to set up both Python and R environments as detailed below.

#### Setting up your R environment

- **Download and Install R:** Visit [CRAN (The Comprehensive R Archive Network)](https://cran.r-project.org/) to download the latest version of R compatible with your operating system. Then verify successful installation via command line:

```
R --version
```
- **Install Necessary R Packages:** Our analysis utilizes several R packages. You can install these packages by starting R (just type `R` in your command line and press enter) and entering the following commands in the R console:
     ```R
     install.packages("package1")
     install.packages("package2")
     #... (continue for all necessary packages)
     ```
- Replace `"package1"`, `"package2"`, etc., with the actual names of the required packages. A list of required R packages is provided in the 'R_Package_Requirements.txt' file.

#### Setting up your python environment

For your python environment, we provide an Anaconda environment file for ease-of-use. 
```
conda env create -f environment.yml
```

> **Warning**:
> To reproduce our exact outputs, you will require GDAL version 3.4. Please be aware that this specific version of GDAL requires a different Python version than the one specified in our environment file. Therefore, we have not included it in the environment file and instead, for the reprojection step, you'll need to set up a new environment:
> ```
> conda create -n gdal_env python=3.10 gdal=3.4
> ```

In order to paralellize the reprojection step, we make use of the [GNU parallel shell tool](https://www.gnu.org/software/parallel/).

```
parallel --version
```

#### The cmethods library

This repository contains a python script used to run debiasing in climate data using a fork of the [original python-cmethods](https://github.com/btschwertfeger/python-cmethods) module written by Benjamin Thomas Schwertfeger's , which has 
been modified to function with the dataset used in the clim-recal project. This library has been included as a 
submodule to this project, so you must run the following command to pull the submodules required.

```
git submodule update --init --recursive
```

#### Downloading the data

**Climate data**

This streamlined pipeline is designed for raw data provided by the Met Office, accessible through the [CEDA archive]((https://catalogue.ceda.ac.uk/uuid/ad2ac0ddd3f34210b0d6e19bfc335539)). It utilizes [UKCP](https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/2.2km) control, scenario data at 2.2km resolution, and [HADs](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km) observational data. For those unfamiliar with this data, refer to our [the dataset](#the-dataset) section.

To access the data,[register here]((https://archive.ceda.ac.uk/)) at the CEDA archive and configure your FTP credentials in "My Account". Utilize our [ceda_ftp_download.py](python/data_download/) script to download the data.

```
# cpm data
python3 ceda_ftp_download.py --input /badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/ --output 'output_dir' --username 'uuu' --psw 'ppp' --change_hierarchy

# hads data
python3 ceda_ftp_download.py --input /badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km --output 'output_dir' --username 'uuu' --psw 'ppp'
```
You need to replace `uuu` and `ppp` with your CEDA username and FTP password respectively and replace `output_dir` with the directory you want to write the data to.

The `--change_hierarchy` flag modifies the folder hierarchy to fit with the hierarchy in the Turing Azure file store. This flag only applies to the UKCP data and should not be used with HADs data. You can use the same script without the `--change_hierarchy` flag in order to download files without any changes to the hierarchy.

> 📢 If you are an internal collaborator you can access the raw data as well as intermediate steps through our Azure server. [See here for a How-to]().

**Geospatial data**

In addition to the climate data we use geospatial data to divide the data into smaller chunks. Specifically we use the following datasets for city boundaries:

- Scottish localities boundaries for cropping out Glasgow. Downloaded from [nrscotland.gov.uk](https://www.nrscotland.gov.uk/statistics-and-data/geography/our-products/settlements-and-localities-dataset/settlements-and-localities-digital-boundaries) on 1st Aug 2023

- Major Towns and Cities boundaries for cropping out Manchester. Downloaded from [https://geoportal.statistics.gov.uk/](https://geoportal.statistics.gov.uk/datasets/980da620a0264647bd679642f96b42c1/explore)


### Reprojecting the data
The HADs data and the UKCP projections have different resolution and coordinate system. For example the HADs dataset uses the British National Grid coordinate system.

The first step in our analysis pipeline is to reproject the UKCP datasets to the British National Grid coordinate system. For this purpose, we utilize the Geospatial Data Abstraction Library (GDAL), designed for reading and writing raster and vector geospatial data formats.

> **Warning**:
> Note that, to reproduce our exact pipeline, we switch environments here as explained in the requirements. 
> ```
> conda activate gdal_env
> ```

To execute the reprojection in parallel fashion, run the `reproject_all.sh` script from your shell. As an input to the script replace `path_to_netcdf_files` with the path to the raw netCDF files.

```bash
cd bash
sh reproject_all.sh path_to_netcdf_files
```

### Resampling the data

Resample the HADsUK dataset from 1km to 2.2km grid to match the UKCP reprojected grid. We run the resampling python script specifying the `--input` location of the reprojected files from the previous step, the UKCP `--grid` file an the `--output` location for saving the resampled files.

```
# switch to main environment
conda activate clim-recal

# run resampling
cd ../python/resampling
python resampling_hads.py --input path_to_reprojected --grid path_to_grid_file --output path_to_resampled
```
### Preparing the bias correction and assessment

**Spatial cropping**
Because the bias correction process is computationally intensive, handling large datasets can be challenging and time-consuming. Therefore, to make the pipeline more manageable and efficient, it is recommended to split the data into smaller subsets. For the purposes of our example pipeline, we've opted for reducing the data to individual city boundaries. To crop you need to adjust the paths in `Cropping_Rasters_to_three_cities.R` script to fit 1your own directory sturcture. The cropping is implemented in the `cpm_read_crop` and `hads_read_crop` functions. 

```
Rscript Cropping_Rasters_to_three_cities.R
```
**calibration-validation data split**
For the purpose of assessing our bias correction, we then split our data, both the projection as well as the ground-truth observations by dates. In this example here we calibrate the bias correction based on the years 1981 to 1983. We then use data from year 2010 to validate the quality of the bias correction. You need to replace `path_to_cropped` with the path where the data from the previous cropping step was saved and  `path_to_preprocessed` with the output directory you choose. You can leave the `-v` and `-r` flags as specified below or choose another metric and run if you prefer.

```
cd ../debiasing
python preprocess_data.py --mod path_to_cropped --obs path_to_cropped -v tasmax -r '05' --out path_to_preprocessed --calib_dates 19810101-19831230 --valid_dates 20100101-20101230
```

The preprocess_data.py script also aligns the calendars of the historical simulation data and observed data, ensuring that they have the same time dimension and checks that the observed and simulated historical data have the same dimensions.

> **Note**:
> preprocess_data.py makes use of our custom data loader functions. In [`data_loader/`](python/load_data/data_loader.py) we have written a few functions for loading and concatenating data into a single xarray which can be used for running debiasing methods. Instructions in how to use these functions can be found in python/notebooks/load_data_python.ipynb.


### Applying the bias correction
Note: By March 2023 we have only implemented the [python-cmethods](https://github.com/alan-turing-institute/python-cmethods) library.


The [run_cmethods.py](../debiasing/run_cmethods.py) allow us to adjusts climate biases in climate data using the python-cmethods library. It takes as input observation data (HADs data), control data (historical UKCP data), and scenario data (future UKCP data), 
and applies a correction method to the scenario data. The resulting output is saved as a `.nc` to a specified directory. The script will also produce a time-series and a map plot of the debiased data. To run this you need to replace `path_to_validation_data` with the output directories of the previous step and specify `path_to_corrected_data` as your output directory for the bias corrected data. You can also specify your preferred `bias_correction_method` (e.g. quantile_delta_mapping).

```
python3 run_cmethods.py --input_data_folder path_to_validation_data --out path_to_corrected_data --method bias_correction_method --v 'tas'
```

The run_cmethods.py script loops over the time periods and applies debiasing in periods of 10 years in the following steps:
  - Loads the scenario data for the current time period.
  - Applies the specified correction method to the scenario data.
  - Saves the resulting output to the specified directory.
  - Creates diagnotic figues of the output dataset (time series and time dependent maps) and saves it into the specified directory.

For each 10 year time period it will produce an `.nc` output file with the adjusted data and a time-series plot and a time dependent map plot of the adjusted data. 
    