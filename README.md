# Welcome to the `clim-recal` repository! 

Welcome to clim-recal, a specialized resource designed to tackle systematic errors or biases in Regional Climate Models (RCMs). As researchers, policy-makers, and various stakeholders explore publicly available RCMs, they need to consider the challenge of biases that can affect the accurate representation of climate change signals. Clim-recal provides both a **broad review** of available bias correction methods as well as **practical tutorials** and **guidance** on how to easily apply those methods to various datasets.

Clim-recal is an **Extensive guide to application of BC methods**: 

- Accessible information about the [why and how of bias correction for climate data]()
- Technical resource for application BC methods (see our full pipeline for bias-correction of the ground-breaking local-scale (2.2km)[Convection Permitting Model (CPM)](https://www.metoffice.gov.uk/pub/data/weather/uk/ukcp18/science-reports/UKCP-Convection-permitting-model-projections-report.pdf))
- In partnership with the MetOffice to ensure the propriety, quality, and usability of our work
- Framework for open additions (in planning)

## Table of Contents

2. [Overview: Bias Correction Pipeline](#overview-bias-correction-pipeline)
3. [Documentation](#documentation)
4. [The dataset](#the-dataset)
4. [Why bias correction?](#why-bias-correction)
8. [License](#license)
9. [Contributors](#contributors)

## Overview: Bias Correction Pipeline

Here we provide an example of how to run a debiasing pipeline starting. The pipeline has the following steps:

1. **Set-up & data download**
    *We provide custom scripts to facilitate download of data*
2. **Preprocessing**
    *This includes reprojecting, resampling & splitting the data prior to bias correction*
5. **Apply bias correction**
    *Our pipeline embeds two distinct methods of bias correction*
6. **Assess the debiased data**
    *We have developed a way to assess the quality of the debiasing step across multiple alternative methods*

*see also this [flowchart viz](https://github.com/alan-turing-institute/clim-recal/blob/documentation/docs/pipeline.md) of the pipeline*

### Prerequisites

#### Setting up your environment

Methods can be used with a custom environment, here we provide a Anaconda
environment file for ease-of-use. 
```
conda env create -f environment.yml
```

> **Warning**:
> To reproduce our exact outputs, you will require GDAL version 3.4. Please be aware that this specific version of GDAL requires a different Python version than the one specified in our environment file. Therefore, we have not included it in the environment file and instead, for the reprojection step, you'll need to set up a new environment:
> ```
> conda create -n gdal_env python=3.10 gdal=3.4
> ```

In order to paralellize the reprojection step, we make use of the [GNU parallel shell tool](https://www.gnu.org/software/parallel/).

#### Downloading the data

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

### Reproject the data
The HADs data and the UKCP projections have different resolution and coordinate system. For example the HADs dataset uses the British National Grid coordinate system.

The first step in our analysis pipeline is to reproject the UKCP datasets to the British National Grid coordinate system. For this purpose, we utilize the Geospatial Data Abstraction Library (GDAL), designed for reading and writing raster and vector geospatial data formats.

> **Warning**:
> Note that, to reproduce our exact pipeline, we switch environments here as explained in the requirements. 
> ```
> conda activate gdal_env
> ```

To execute the reprojection in parallel fashion, run the `reproject_all.sh` script from your shell. First, ensure the scripts have the necessary permissions and that the parallel package is installed:

```bash
sh bash/reproject_all.sh
```

### Resample the data

Resample the HADsUK dataset from 1km to 2.2km grid to match the UKCP reprojected grid. We run the resampling python script specifying the `--input` location of the reprojected files from the previous step, the UKCP `--grid` file an the `--output` location for saving the resampled files.

```
python python/resampling/resampling_hads.py --input path_to_reprojected --grid path_to_grid_file --output path_to_resampled

```

### Preparing the bias correction and assessment
**Data loaders** functions for loading and concatenating data into a single xarray which can be used for running debiasing methods.
In [python/load_data/data_loader.py] we have written a few functions for loading and concatenating data into a single xarray which can be used for running debiasing methods. Instructions in how to use these functions can be found in python/notebooks/load_data_python.ipynb.

### Applying the bias correction
  - **Debiasing scripts** that interface with implementations of the debiasing (bias correction) methods implemented by different libraries (by March 2023 we have only implemented the python-cmethods library).

  The code in the [debiasing](debiasing) directory contains scripts that interface with implementations of the debiasing methods 
implemented by different libraries.

Note: By March 2023 we have only implemented the [python-cmethods](https://github.com/alan-turing-institute/python-cmethods) library.


### The cmethods library

This repository contains a python script used to run debiasing in climate data using a fork of the [original python-cmethods](https://github.com/btschwertfeger/python-cmethods) module written by Benjamin Thomas Schwertfeger's , which has 
been modified to function with the dataset used in the clim-recal project. This library has been included as a 
submodule to this project, so you must run the following command to pull the submodules required.

```
cd debiasing
git submodule update --init --recursive
```

The [run_cmethods.py](debiasing/run_cmethods.py) allow us to adjusts climate biases in climate data using the python-cmethods library. 
It takes as input observation data (HADs data), control data (historical UKCP data), and scenario data (future UKCP data), 
and applies a correction method to the scenario data. The resulting output is saved as a `.nc` to a specified directory.
The script will also produce a time-series and a map plot of the debiased data.

**Usage**:

The script can be run from the command line using the following arguments:

```
python3 run_cmethods.py.py --obs <path to observation datasets> --contr <path to control datasets> --scen <path to scenario datasets> --shp <shapefile> 
--out <output file path> -m <method> -v <variable> -u <unit> -g <group> -k <kind> -n <number of quantiles> -p <number of processes>
```

where:

where:

- `--obs` specifies the path to the observation datasets
- `--contr` specifies the path to the control datasets
- `--scen`  specifies the path to the scenario datasets (data to adjust)
- `--shp`  specifies the path to a shapefile, in case we want to select a smaller region (default: None)
- `--out` specifies the path to save the output files (default: current directory)
- `--method` specifies the correction method to use (default: quantile_delta_mapping)
- `-v` specifies the variable to adjust (default: tas)
- `-u`  specifies the unit of the variable (default: °C)
- `-g`  specifies the value grouping (default: time)
- `-k`  specifies the method kind (+ or *, default: +)
- `-n`  specifies the number of quantiles to use (default: 1000)
- `-p`  specifies the number of processes to use for multiprocessing (default: 1)

For more details on the script and options you can run:

```
python run_cmethods.py --help
```
**Main Functionality**:

The script applies corrections extracted from historical observed and simulated data between `1980-12-01` and `1999-11-30`.
Corrections are applied to future scenario data between `2020` and `2080` (however there is no available scenario data between `2040` to `2060`, so this time
period is skipped.


The script performs the following steps:

- Parses the input arguments.
- Loads, merges and clips (if shapefile is provided) the all input datasets and merges them into two distinct datasets: the observation and control datasets.
- Aligns the calendars of the historical simulation data and observed data, ensuring that they have the same time dimension 
and checks that the observed and simulated historical data have the same dimensions.
- Loops over the future time periods specified in the `future_time_periods` variable and performs the following steps:
  - Loads the scenario data for the current time period.
  - Applies the specified correction method to the scenario data.
  - Saves the resulting output to the specified directory.
  - Creates diagnotic figues of the output dataset (time series and time dependent maps) and saves it into the specified directory.

In this script 
datasets are debiased in periods of 10 years, in a consecutive loop, for each time period it will produce an `.nc` output file
with the adjusted data and a time-series plot and a time dependent map plot of the adjusted data. 

**Working example**.

Example of code working on the **clim-recal** dataset:
```
python run_cmethods.py --scen /Volumes/vmfileshare/ClimateData/Reprojected/UKCP2.2/tasmax/01/latest --contr /Volumes/vmfileshare/ClimateData/Reprojected/UKCP2.2/tasmax/01/latest/ --obs /Volumes/vmfileshare/ClimateData/Processed/HadsUKgrid/resampled_2.2km/tasmax/day/ --shape ../../data/Scotland/Scotland.bbox.shp -v tasmax --method delta_method --group time.month -p 5
```
    
### Assessing the corrected data

## Documentation
🚧 In Progress

We are in the process of developing comprehensive documentation for our codebase to supplement the guidance provided in this document. In the interim, for Python scripts, you can leverage the inline documentation (docstrings) available within the code. To access a summary of the available options and usage information for any Python script, you can use the `--help` flag in the command line as follows:

  ```sh
  python <script_name>.py --help
  ```
  For example:
  ```sh
  python resampling_hads.py --help

  usage: resampling_hads.py [-h] --input INPUT [--output OUTPUT] [--grid_data GRID_DATA]

  options:
  -h, --help            show this help message and exit
  --input INPUT         Path where the .nc files to resample is located
  --output OUTPUT       Path to save the resampled data data
  --grid_data GRID_DATA
                        Path where the .nc file with the grid to resample is located
  ```
This will display all available options for the script, including their purposes.

For R scripts, please refer to the comments within the R scripts for contextual information and usage guidelines, and feel free to reach out with any specific queries.

We appreciate your patience and encourage you to check back for updates on our ongoing documentation efforts.
## The dataset

### UKCP18
The UK Climate Projections 2018 (UKCP18) dataset offers insights into the potential climate changes in the UK. UKCP18 is an advancement of the UKCP09 projections and delivers the latest evaluations of the UK's possible climate alterations in land and marine regions throughout the 21st century. This crucial information aids in future Climate Change Risk Assessments and supports the UK’s adaptation to climate change challenges and opportunities as per the National Adaptation Programme.

### HADS
[HadUK-Grid](https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid/haduk-grid) is a comprehensive collection of climate data for the UK, compiled from various land surface observations across the country. This data is organized into a uniform grid to ensure consistent coverage throughout the UK at up to 1km x 1km resolution. The dataset, spanning from 1836 to the present, includes a variety of climate variables such as air temperature, precipitation, sunshine, and wind speed, available on daily, monthly, seasonal, and annual timescales. 

## Why bias correction?

Regional climate models (RCMs) contain systematic errors, or biases in their output [1]. Biases arise in RCMs for a number of reasons, such as the assumptions in the general circulation models (GCMs), and in the downscaling process from GCM to RCM [1,2].

Researchers, policy-makers and other stakeholders wishing to use publicly available RCMs need to consider a range of "bias correction” methods (sometimes referred to as "bias adjustment" or "recalibration"). Bias correction methods offer a means of adjusting the outputs of RCM in a manner that might better reflect future climate change signals whilst preserving the natural and internal variability of climate [2]. 

Part of the clim-recal project is to review several bias correction methods. This work is ongoing and you can find our initial [taxonomy here](https://docs.google.com/spreadsheets/d/18LIc8omSMTzOWM60aFNv1EZUl1qQN_DG8HFy1_0NdWk/edit?usp=sharing). When we've completed our literature review, it will be submitted for publication in an open peer-reviewed journal. 

Our work is however, just like climate data,  intended to be dynamic, and we are in the process of setting up a pipeline for researchers creating new methods of bias correction to be able to submit their methods for inclusion on in the **clim-recal** repository. 

 1. Senatore et al., 2022, https://doi.org/10.1016/j.ejrh.2022.101120 
 2. Ayar et al., 2021, https://doi.org/10.1038/s41598-021-82715-1 


### Let's collaborate!

We hope to bring together the extensive work already undertaken by the climate science community and showcase a range of libraries and techniques. If you have suggestions on the repository, or would like to include a new method (see below) or library, please raise an issue or [get in touch](mailto:clim-recal@turing.ac.uk)! 

### Adding to the conda environment file 

To use `R` in anaconda you may need to specify the `conda-forge` channel:

```
conda config --env --add channels conda-forge
```

Some libraries may be only available through `pip`, for example, these may
require the generation / update of a `requirements.txt`:

```
pip freeze > requirements.txt
```

and installing with:

```
pip install -r requirements.txt
```

## 🚧 Future plans

- **More BC Methods**: Further bias correction of UKCP18 products. *This is planned for a future release and is not available yet.*
- **Pipeline for adding new methods**: *This is planned for a future release and is not available yet.*

## License

## Contributors
