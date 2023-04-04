# Welcome to the `clim-recal` repository! 

## Background

Regional climate models (RCMs) contain systematic errors, or biases in their output [1]. Biases arise in RCMs for a number of reasons, such as the assumptions in the general circulation models (GCMs), and in the downscaling process from GCM to RCM [1,2].

Researchers, policy-makers and other stakeholders wishing to use publicly available RCMs need to consider a range of "bias correction” methods (sometimes referred to as "bias adjustment" or "recalibration"). Bias correction methods offer a means of adjusting the outputs of RCM in a manner that might better reflect future climate change signals whilst preserving the natural and internal variability of climate [2]. 

The aim of **clim-recal** is therefore to: 

* To provide non-climate scientists with an extensive guide to the application, disadvatanges/advantages and use of BC methods 
* To provide researchers with a collated set of resources for how to technically apply the BC methods, with a framework for open additions 
* To create accessible information on bias adjustment methods for non quantititative researchers and lay-audience stakeholders 

We are working in partnership with the MetOffice to ensure the propriety of our work. We're focusing on the UKCP18 suite of products, with the first dataset of focus their ground-breaking local-scale (2.2km) [Convection Permitting Model (CPM)](https://www.metoffice.gov.uk/pub/data/weather/uk/ukcp18/science-reports/UKCP-Convection-permitting-model-projections-report.pdf). 

### We love to collaborate!

We hope to bring together the extensive work already undertaken by the climate science community and showcase a range of libraries and techniques. If you have suggestions on the repository, or would like to include a new method (see below) or library, please raise an issue or get in touch! **(can we set up a clim-recal [at] turing.ac.uk

### Methods taxonomy 

Our work-in-progress taxonomy can be viewed [here](https://docs.google.com/spreadsheets/d/18LIc8omSMTzOWM60aFNv1EZUl1qQN_DG8HFy1_0NdWk/edit?usp=sharing). When we've completed our literature review, it will be submitted for publication in an open peer-reviewed journal. 

Our work is however, just like climate data,  intended to be dynamic, and we are in the process of setting up a pipeline for researchers creating new methods of bias correction to be able to submit their methods for inclusion on in the **clim-recal** repository. 


## Code

In this repo we aim to provide examples of how to run the debiasing pipeline starting from the raw data avalaible from the [MET office via CEDA](https://catalogue.ceda.ac.uk/uuid/ad2ac0ddd3f34210b0d6e19bfc335539) to the creation of debiased (bias corrected) datasets for different time periods. The pipeline has the following steps:

1. Reproject the [UKCP](https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/2.2km) control and scenario data to the same coordinate system as the [HADs](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km) observational data (British National Grid).
2. Resample the HADs data from 1km to 2.2km grid to match the UKCP reprojected grid.
3. Run debiasing method on the control and observational data and project it into the scenario dataset. 

After each of these steps the reprojected, resampled and debiased scenario datasets are produced and saved in an Azure fileshare storage (more details about this bellow).


### Bash

Here you find scripts to reproject the UKCP datasets to the British National Grid coordinate system.

### Python

In the `python` subdirectory you can find code for the different steps data processing and debiasing:
    - **Resampling** for the HADsUK datasets from 1km to a 2.2 km grid to match the UKCP re-projected grid.
    - **Data loaders** functions for loading and concatenating data into a single xarray which can be used for running debiasing methods.
    - **Debiasing scripts** that interface with implementations of the debiasing (bias correction) methods implemented by different libraries (by March 2023 we have only implemented the python-cmethods library).
    
More details in how to use this code can be found in [the python README file](python/README.md) and the environment used in this [environment setup file](setup-instructions.md).

### R 

In the `R` subdirectory you can find code for replicating the different data processing and debiasing steps as above, along with comparisons of methods between the two languages. 
- **bias-correction-methods** for bias correction (debiasing) methods availble specifically in `R` libraries
- **comparing-r-and-python** for replication of resampling and reviewing the bias correction methods applied in `python`.
- **Resampling** for resampling the HADsUK datasets from 1km to 2.2km grid in `R`.


## Data

### Accesing the data

Datasets used in this project (raw, processed and debiased) are being stored in an Azure fileshare set-up for the clim-recal project (https://dymestorage1.file.core.windows.net/vmfileshare). You need to be given access, and register your IP adress to the approve list in the following way from the azure portal:

- Go to dymestorage1 page
- Security + networking tab
- Add your IP under the Firewall section


Once you have access you can mount the fileshare. On a Mac you can do it from  a terminal 

`open smb://dymestorage1.file.core.windows.net/vmfileshare`

username is `dymestorage1` and the password can be found in the access keys as described in [here](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys).

The fileshare will be mounted under

`/Volumes/vmfileshare/`

Instructions of how the mount in other operating systems can be found in [the azure how-tos](https://learn.microsoft.com/en-us/azure/storage/files/storage-how-to-use-files-linux?tabs=smb311).

### Dataset description

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
└── shapefiles
    ├── Middle_Layer_Super_Output_Areas_(December_2011)_Boundaries
    └── infuse_ctry_2011_clipped
```

## Future directions

In future, we're hoping to include:

- Further bias correction of UKCP18 products 
- Assessment of the influence of different observational data 
- Pipelines for adding an additional method 

## References

 1. Senatore et al., 2022, https://doi.org/10.1016/j.ejrh.2022.101120 
 2. Ayar et al., 2021, https://doi.org/10.1038/s41598-021-82715-1 
