# Welcome to the `clim-recal` repository! 


Welcome to clim-recal, a specialized resource designed to tackle systematic errors or biases in Regional Climate Models (RCMs). As researchers, policy-makers, and various stakeholders explore publicly available RCMs, they need to consider the challenge of biases that can affect the accurate representation of climate change signals. Clim-recal provides both a **broad review** of available bias correction methods as well as **practical tutorials** and **guidance** on how to easily apply those methods to various datasets.

Clim-recal is an **Extensive guide to application of BC methods**: 

- Accessible information for non quantitative researchers and lay-audience stakeholders 
- Technical resource for application BC methods
- Framework for open additions
- In partnership with the MetOffice to ensure the propriety, quality, and usability of our work
- Full pipeline for bias-corrected data of the ground-breaking local-scale (2.2km)[Convection Permitting Model (CPM)](https://www.metoffice.gov.uk/pub/data/weather/uk/ukcp18/science-reports/UKCP-Convection-permitting-model-projections-report.pdf). 


## Table of Contents

1. [Introduction](#)
2. [Quick Start Guide](#quick-start-guide)
4. [Guidance for Non-Climate Scientists](#guidance-non-expert)
5. [Guidance for Climate Scientists](#guidance-expert)
6. [Documentation](#documentation)
    - [The data](#data-download)
    - [Python Pipeline](#python-pipeline)
    - [R Pipeline](#r-pipeline)
    - [FAQs](#faqs)
6. [Research](#research)
    - [Literature Review](#review)
    - [Full BC Taxonomy](#taxonomy)
    - [References](#references)
7. [License](#contributors)
8. [Contributors](#license)


## Quick Start Guide

- should we include a toy dataset or simulated data?
- this should also be available in form of notebook


## Guidance for Non-Climate Scientists

Regional climate models (RCMs) contain systematic errors, or biases in their output [1]. Biases arise in RCMs for a number of reasons, such as the assumptions in the general circulation models (GCMs), and in the downscaling process from GCM to RCM [1,2].

Researchers, policy-makers and other stakeholders wishing to use publicly available RCMs need to consider a range of "bias correction” methods (sometimes referred to as "bias adjustment" or "recalibration"). Bias correction methods offer a means of adjusting the outputs of RCM in a manner that might better reflect future climate change signals whilst preserving the natural and internal variability of climate [2]. 

## Guidance for Climate Scientists

### How to link this with your data?

### Let's collaborate!

We hope to bring together the extensive work already undertaken by the climate science community and showcase a range of libraries and techniques. If you have suggestions on the repository, or would like to include a new method (see below) or library, please raise an issue or [get in touch](mailto:clim-recal@turing.ac.uk)! 

## Documentation

### Code

In this repo we aim to provide examples of how to run the debiasing pipeline starting from the raw data available from the [MET office via CEDA](https://catalogue.ceda.ac.uk/uuid/ad2ac0ddd3f34210b0d6e19bfc335539) to the creation of debiased (bias corrected) datasets for different time periods. The pipeline has the following steps:

1. Reproject the [UKCP](https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/2.2km) control and scenario data to the same coordinate system as the [HADs](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km) observational data (British National Grid).
2. Resample the HADs data from 1km to 2.2km grid to match the UKCP reprojected grid.
3. Run debiasing method on the control and observational data and project it into the scenario dataset. 

After each of these steps the reprojected, resampled and debiased scenario datasets are produced and saved in an Azure fileshare storage (more details about this bellow).


### Bash

Here you find scripts to reproject the UKCP datasets to the British National Grid coordinate system.

### Python

In the `python` subdirectory you can find code for the different data download, processing and debiasing steps:
   - **Data download** for a script to download data from the CEDA archive.
   - **Resampling** for the HADsUK datasets from 1km to a 2.2 km grid to match the UKCP re-projected grid.
   - **Data loaders** functions for loading and concatenating data into a single xarray which can be used for running debiasing methods.
   - **Debiasing scripts** that interface with implementations of the debiasing (bias correction) methods implemented by different libraries (by March 2023 we have only implemented the python-cmethods library).
    
More details in how to use this code can be found in [the python README file](python/README.md) and the environment used in this [environment setup file](setup-instructions.md).

### R 

In the `R` subdirectory you can find code for replicating the different data processing and debiasing steps as above, along with comparisons of methods between the two languages. 
- **bias-correction-methods** for bias correction (debiasing) methods available specifically in `R` libraries
- **comparing-r-and-python** for replication of resampling and reviewing the bias correction methods applied in `python`.
- **Resampling** for resampling the HADsUK datasets from 1km to 2.2km grid in `R`.

## Data access

### How to download the data

You can download the raw UKCP2.2 climate data from the CEDA archive. Go [here](https://archive.ceda.ac.uk/), create an account and set up your FTP credentials in "My Account". You can then use the python script under `python/data_download/` to download the data: 
```
python3 ceda_ftp_download.py --input /badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/ --output 'output_dir' --username 'uuu' --psw 'ppp' --change_hierarchy
```
You need to replace `uuu` and `ppp` with your CEDA username and FTP password respectively and replace 'output_dir' with the directory you want to write the data to.

Note that the `--change_hierarchy` flag is used, which modifies the folder hierarchy to fit with the hierarchy in the Turing Azure file store. You can use the same script without the `--change_hierarchy` flag in order to download files without any changes in the hierarchy.

You can download the HADs observational data from the CEDA archive using the same python script, with a different input (note the `change_hierarchy` flag should not be used with HADs data - only applies to UKCP data):
```
python3 ceda_ftp_download.py --input /badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km --output output_dir --username 'uuu' --psw 'ppp'
```

### Accessing the pre-downloaded/pre-processed data

Datasets used in this project (raw, processed and debiased) have been pre-downloaded/pre-processed and stored in an Azure fileshare set-up for the clim-recal project (https://dymestorage1.file.core.windows.net/vmfileshare). You need to be given access, and register your IP address to the approved list in the following way from the azure portal:

- Go to dymestorage1 page `Home > Storage accounts > dymestorage1`
- Navigate to *Networking* tab under Security + networking
- Add your IP under the Firewall section

Once you have access you can mount the fileshare. On a Mac you can do it from a terminal:

`open smb://dymestorage1.file.core.windows.net/vmfileshare`

username is `dymestorage1` and the password can be found in the access keys as described in [here](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys).

The fileshare will be mounted under

`/Volumes/vmfileshare/`

Instructions on how the mount in other operating systems can be found in [the azure how-tos](https://learn.microsoft.com/en-us/azure/storage/files/storage-how-to-use-files-linux?tabs=smb311). 

Alternatively, you can access the Azure Portal, go to the dymestorage1 fileshare and click the "Connect" button to get an automatically generated script. This script can be used from within an Azure VM to mount the drive.

### Pre-downloaded/pre-processed data description

All the data used in this project can be found in the `/Volumes/vmfileshare/ClimateData/` directory. 

```
.
├── Debiased  # Directory where debiased datasets are stored.
│   └── tasmax
├── Processed # Directory where processed climate datasets are stored. 
│   ├── CHESS-SCAPE
│   ├── HadsUKgrid # Resampled HADs grid.
│   └── UKCP2.2_Reproj # Old reprojections (to delete).
├── Raw # Raw climate data
│   ├── CHESS-SCAPE
│   ├── HadsUKgrid
│   ├── UKCP2.2
│   └── ceda_fpt_download.py # script to download data from CEDA database. 
├── Reprojected # Directory where reprojected UKCP datasets are stored.
│   └── UKCP2.2
├── Reprojected_infill # Directory where reprojected UKCP datasets are stored, including the newest infill UKCP2.2 data published in May 2023.
└── shapefiles
    ├── Middle_Layer_Super_Output_Areas_(December_2011)_Boundaries
    └── infuse_ctry_2011_clipped
```

## Research
### Literature Review
    
### Methods taxonomy 

Our work-in-progress taxonomy can be viewed [here](https://docs.google.com/spreadsheets/d/18LIc8omSMTzOWM60aFNv1EZUl1qQN_DG8HFy1_0NdWk/edit?usp=sharing). When we've completed our literature review, it will be submitted for publication in an open peer-reviewed journal. 

Our work is however, just like climate data,  intended to be dynamic, and we are in the process of setting up a pipeline for researchers creating new methods of bias correction to be able to submit their methods for inclusion on in the **clim-recal** repository. 

## Future directions

In future, we're hoping to include:

- Further bias correction of UKCP18 products 
- Assessment of the influence of different observational data 
- Pipelines for adding an additional method 

## References

 1. Senatore et al., 2022, https://doi.org/10.1016/j.ejrh.2022.101120 
 2. Ayar et al., 2021, https://doi.org/10.1038/s41598-021-82715-1 

## License

## Contributors
