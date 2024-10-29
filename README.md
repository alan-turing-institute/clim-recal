# Welcome to the `clim-recal` repository!

Welcome to `clim-recal`, a specialized resource designed to tackle systematic errors or biases in **Regional Climate Models (RCMs)**. As researchers, policy-makers, and various stakeholders explore publicly available RCMs, they need to consider the challenge of biases that can affect the accurate representation of climate change signals.

`clim-recal` provides both a **broad review** of available bias correction methods as well as **software**, **practical tutorials** and **guidance** that helps users apply these methods methods to various datasets.

`clim-recal` is an **extensive software library and guide to application of Bias Correction (BC) methods**:

- Contains accessible information about the [why and how of bias correction for climate data](#why-bias-correction)
- Is a software library for for the application of BC methods (see our full pipeline for bias-correction of the ground-breaking local-scale (2.2km) [Convection Permitting Model (CPM)](https://www.metoffice.gov.uk/pub/data/weather/uk/ukcp18/science-reports/UKCP-Convection-permitting-model-projections-report.pdf). `clim-recal` brings together different software packages in `python` and `R` that implement a variety of bias correction methods, making it easy to apply them to data and compare their outputs.
- Was developed in partnership with the MetOffice to ensure the propriety, quality, and usability of our work
- Provides a framework for open additions of new software libraries/bias correction methods (in planning)

*WARNING*: The documentation below is a work in progress and we are updating it to reflect recent significant changes.
<!-- In the meantime, see [Exported Datasets](/docs/datasets.qmd) page for updates on datasets released for three UK cities.-->

<!-- # Data results and documentation updates -->


<!-- ## Table of Contents -->
<!---->
<!-- 1. [Overview: Bias Correction Pipeline](#overview-bias-correction-pipeline) -->
<!-- 1. [Documentation](#documentation) -->
<!-- 1. [The Datasets](#the-datasets) -->
<!-- 1. [Why Bias Correction?](#why-bias-correction) -->
<!-- 1. [Contributing](#contributing) -->
<!-- 1. [Future Plans](#future-plans) -->
<!-- 1. [License](/LICENSE) -->

# Overview: Bias Correction Pipeline

`clim-recal` is a debiasing pipeline,  with the following steps:

1. **Set-up & data download**
    *We provide custom scripts to facilitate download of data*
2. **Preprocessing**
    *This includes reprojecting, resampling & splitting the data prior to bias correction*
3. **Apply bias correction**
    *Our pipeline embeds two distinct methods of bias correction*
4. **Assess the debiased data**
    *We have developed a way to assess the quality of the debiasing step across multiple alternative methods*

For a quick start on bias correction, refer to our [comprehensive analysis pipeline guide](https://github.com/alan-turing-institute/clim-recal/blob/documentation/docs/pipeline_guidance.md).

# Documentation

We are in the process of developing comprehensive documentation for our code base to supplement the guidance provided in this and other `README.md` files. In the interim, there is documentation available in the following forms:

## User documentation

- See [setup instructions](setup-instructions)
- See `python` [`README`](python/README) for an overview of the pipeline
- Once installed, using the `clim-recal --help` option for details
- See the [reproducibility page](docs/reproducibility) for information on how we used clim-recal

## To use `clim-recal` programmatically

- There are extensive [`API Reference`](docs/reference/) within the python code.
- Comments within `R` scripts

## To contribute to `clim-recal`

- See the [Contributing](docs/contributing) section below

<!---->
<!---->
<!-- - The `clim-recal` `python` package in the `python/folder` -->
<!-- - Prior scripts  -->
<!---->
<!-- For many of our `python` command line scripts, you can use the `--help` flag to access a summary of the available options and usage information: -->
<!---->
<!-- ```sh -->
<!-- $ python resampling_hads.py --help -->
<!---->
<!-- usage: resampling_hads.py [-h] --input INPUT [--output OUTPUT] [--grid_data GRID_DATA] -->
<!---->
<!-- options: -->
<!-- -h, --help            show this help message and exit -->
<!-- --input INPUT         Path where the .nc files to resample is located -->
<!-- --output OUTPUT       Path to save the resampled data data -->
<!-- --grid_data GRID_DATA Path where the .nc file with the grid to resample is located -->
<!-- ``` -->
<!---->
<!-- This will display all available options for the script, including their purposes. -->


# The Datasets

## UKCP18
The [UK Climate Projections 2018 (UKCP18)](https://www.metoffice.gov.uk/research/approach/collaboration/ukcp) dataset offers insights into the potential climate changes in the UK. UKCP18 is an advancement of the UKCP09 projections and delivers the latest evaluations of the UK's possible climate alterations in land and marine regions throughout the 21st century. This crucial information aids in future Climate Change Risk Assessments and supports the UK’s adaptation to climate change challenges and opportunities as per the National Adaptation Programme.

## HADS
[HadUK-Grid](https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid/haduk-grid) is a comprehensive collection of climate data for the UK, compiled from various land surface observations across the country. This data is organized into a uniform grid to ensure consistent coverage throughout the UK at up to 1km x 1km resolution. The dataset, spanning from 1836 to the present, includes a variety of climate variables such as air temperature, precipitation, sunshine, and wind speed, available on daily, monthly, seasonal, and annual timescales.

<!---->
<!-- ### Geographical Dataset -->
<!-- The geographical dataset can be used for visualising climate data. It mainly includes administrative boundaries published by the Office for National Statistics (ONS). The dataset is sharable under the [Open Government Licence v.3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) and is available for download via this [link](https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom_2022/FeatureServer/replicafilescache/NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom_2022_7279368953270783580.zip). We include a copy in the `data/Geofiles` folder for convenience. In addition, the clips for three cities' boundaries from the same dataset are copied to `three.cities` subfolder. -->

# Why Bias Correction?

Regional climate models contain systematic errors, or biases in their output [^1]. Biases arise in RCMs for a number of reasons, such as the assumptions in the general circulation models (GCMs), and in the downscaling process from GCM to RCM.

Researchers, policy-makers and other stakeholders wishing to use publicly available RCMs need to consider a range of "bias correction” methods (sometimes referred to as "bias adjustment" or "recalibration"). Bias correction methods offer a means of adjusting the outputs of RCM in a manner that might better reflect future climate change signals whilst preserving the natural and internal variability of climate [^2].

Part of the `clim-recal` project is to review several bias correction methods. This work is ongoing and you can find our initial [taxonomy here](https://docs.google.com/spreadsheets/d/18LIc8omSMTzOWM60aFNv1EZUl1qQN_DG8HFy1_0NdWk/edit?usp=sharing). When we've completed our literature review, it will be submitted for publication in an open peer-reviewed journal.

Our work is however, just like climate data, intended to be dynamic, and we are in the process of setting up a pipeline for researchers creating new methods of bias correction to be able to submit their methods for inclusion on in the `clim-recal` repository.

[^1]: Senatore et al., 2022, <https://doi.org/10.1016/j.ejrh.2022.101120>
[^2]: Ayar et al., 2021, <https://doi.org/10.1038/s41598-021-82715-1>


# Contributing

If you have suggestions on the repository, or would like to include a new method (see below) or library, please
- raise an [issue](https://github.com/alan-turing-institute/clim-recal/issues)
- [get in touch](mailto:clim-recal@turing.ac.uk)
- see our [contributing](docs/contributing) section, which includes details on contriubting to the documentation.

All are welcome and appreciated.

<!-- ### Adding to the conda environment file -->
<!---->
<!-- To use `R` in `anaconda` you may need to specify the `conda-forge` channel: -->
<!---->
<!-- ```sh -->
<!-- $ conda config --env --add channels conda-forge -->
<!-- ``` -->
<!---->
<!-- Some libraries may be only available through `pip`, for example, these may -->
<!-- require the generation / update of a `requirements.txt`: -->
<!---->
<!-- ```sh -->
<!-- $ pip freeze > requirements.txt -->
<!-- ``` -->
<!---->
<!-- and installing with: -->
<!---->
<!-- ```sh -->
<!-- $ pip install -r requirements.txt -->
<!-- ``` -->

# Future plans
- **Finish refactor for BC**: The infrastructure for testing bias correction methods needs some reworking and documentation.
- **Release BC results**: Provide results from example BC runs.
- **More BC Methods**: Further bias correction of UKCP18 products. *This is planned for a future release and is not available yet.*
- **Pipeline for adding new methods**: *This is planned for a future release and is not available yet.*
