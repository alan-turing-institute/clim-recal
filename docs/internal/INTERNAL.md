> **Note:** This document is intended for internal collaborators of clim-recal. It provides additional instructions and information that are relevant to the internal development and collaboration process.

# Instructions for internal collaborators
## Accessing the pre-downloaded/pre-processed data

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
## Running the pipeline

> **Placeholder**:
> Creating an azure virtual machine outside the DYME-CHH azure resource group may cause
> permission errors in mounting vmfileshare from dymestorage1.

### Reprojection

In order to run the [reprojection step](https://github.com/alan-turing-institute/clim-recal/tree/documentation#reproject-the-data) of the pipeline on the Azure VM there are some additional steps that need to be taken: You need to set permissions and install the parallel package.

```
chmod +x ./reproject_one.sh
chmod +x ./reproject_all.sh
sudo apt-get update
sudo apt-get install parallel
```
