# Installation

See [setup instructions](../setup-instructions.md) for detailed installation options, including via the `python` package `pdm`, `conda` and `docker`.

If the `python` `clim-recal` package is installed, some of the components can be used via the command line. For details run:

```sh
$ clim-recal --help
```

# Pipeline

There are three core steps we hope to implement in our `python` `clim-recal` package:

1. Download UK Met Office `observation` and `projection` data
2. Convert that data to aligned spatial time series grids for debiasing
3. Facilitate running and comparing debiasing methods

We currently provide the the following coverted and aligned files in our [Download Datasets](docs/download) page:

- Maximum temparture: `tasmax`
- Maximum temparture: `tasmin`
- Rainfall: `rainfall`/`pr`

We provide 5 projection runs:

- `01`
- `05`
- `06`
- `07`
- `08`

We demonstrate the advantage of focusing on `05` through `08` in [Identifying Runs](R/misc/Identifying_Runs).

Below is how these files are produced.

## Download UK Met Office data

We use two datasets maintained by the [Met Office](https://www.metoffice.gov.uk)

- [UKHAD observational data](https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km)
- [RCP8.5 projection data](https://data.ceda.ac.uk/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/)

This data can be quite large and take significant amounts of time to download.

To download this an account is needed. The registration process can be found here: <https://services.ceda.ac.uk/cedasite/register/info/>.

We hope to incorporate this module in the `clim-recal` command line interface in future, but at present the `ceda_ftp_download.py` module is used to download this data and can be run from the command line via `python3`:

```sh
$ cd clim-recal/python/clim_recal
$ ./ceda_ftp_download.py  --help
usage: ceda_ftp_download.py [-h] --input INPUT [--output OUTPUT] --username
                            USERNAME --psw PSW [--reverse] [--shuffle]
                            [--change_hierarchy]

options:
  -h, --help           show this help message and exit
  --input INPUT        Path where the CEDA data to download is located. This can be
                       a path with or without subdirectories. Set to
                       `/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/` to download all
                       the raw UKCP2.2 climate projection data used in clim-recal.
  --output OUTPUT      Path to save the downloaded data
  --username USERNAME  Username to connect to the CEDA servers
  --psw PSW            FTP password to authenticate to the CEDA servers
  --reverse            Run download in reverse (useful to run downloads in
                       parallel)
  --shuffle            Run download in shuffle mode (useful to run downloads in
                       parallel)
  --change_hierarchy   Change the output sub-directories' hierarchy to fit the
                       Turing Azure fileshare hierarchy (only applicable to UKCP
                       climate projection data, i.e. when --input is set to
```

## Aligning HADs and CPM UK data

These dataset are provided in the following forms

| Dataset | Resolution | Coordinates                        | Calendar   |
| ---:    | :---:      | :---:                              |  :---:     |
| UKHAD   | 1 km       |  British National Grid[^hads-grid] | Standard   |
| RCP8.5  | 2.2 km     |  Rotated Pole[^cpm-grid]           | 360 day    |

[^hads-grid]: British National Grid spec: <https://epsg.io/27700>
[^cpm-grid]: Rotated pole of latitude `37.5` and longitude `177.5`. See [_UKCP Guidance: Data availability, access and formats_](https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/research/ukcp/ukcp18_guidance_data_availability.pdf) appendix B.

To align them we:

1. Interpolate RCP8.5 to standard gregorian calendar via the `nearest` method[^nearest].
1. Reproject RCP8.5 to British National Grid Coordinate structure.
1. Resample UKHAD to to 2.2 km
1. Reproject UKHAD bounds to align with RCP8.5

Links to these files are in the [Download Datasets](docs/downloads) section.

[^nearest]: See the [CPM Projection](docs/cpm_projection) section for more details, including a comparison with a linear interpolation.

## Running debiasing methods

Preliminary work in this process is in the `clim_recal/debiasing` section. We are at present assessing future options for this, likely via either

- [`ibicus`](https://ibicus.readthedocs.io/en/latest/)
- [`python-cmethods`](https://pypi.org/project/python-cmethods/)
