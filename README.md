# Welcome to the `clim-recal` repository!
![mit-license](https://img.shields.io/github/license/alan-turing-institute/clim-recal)
![coverage](https://alan-turing-institute.github.io/clim-recal/assets/coverage.svg)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
![CI](https://github.com/alan-turing-institute/clim-recal/actions/workflows/ci.yaml/badge.svg)

Welcome to `clim-recal`, a specialised resource designed to prepare data to tackle systematic errors or biases in **Regional Climate Models (RCMs)**. As researchers, policy-makers, and various stakeholders explore publicly available RCMs, they may wish to consider the challenge of biases that can affect the accurate representation of climate change signals.

`clim-recal` provides a data-processing pipeline for extracting parts of the **UK Climate Projections 2018 Convection Permitting model (UKCP18-CPM)** in order to apply and assess **bias correction methods**. 

In future, our [sister project](https://github.com/Urban-Analytics-Technology-Platform/bias-correction-for-cpm) aims to provide both a **broad review** of available **bias-correction methods** as well as assessment of these **methods** and **software** that helps users apply these methods methods to various datasets.The results of this work may then be integrated back to `clim-recal`. 

`clim-recal:`

- Is a software library for pre-processing climate data to ready it for bias-correction (see our full pipeline for bias-correction of the ground-breaking local-scale (2.2km) [Convection Permitting Model (CPM)](https://www.metoffice.gov.uk/pub/data/weather/uk/ukcp18/science-reports/UKCP-Convection-permitting-model-projections-report.pdf). `clim-recal` brings together different software packages in `python` and `R` that implement a variety of bias correction methods, making it easy to apply them to data and compare their outputs.
- Was developed in partnership with the MetOffice to ensure the propriety, quality, and usability of our work
- Provides a framework for open additions of new software libraries/bias correction methods (in planning)

# Overview: Data-processing Pipeline

`clim-recal` is a data-processing pipeline,  with the following steps:

1. **Set-up & data download**
    *We provide custom scripts to facilitate download of data*
2. **Preprocessing**
    *This includes reprojecting, resampling & splitting the data prior to bias correction*
    
Our team are currently working on applying and assessing different methods of bias correction. There's lots of collaboration opportunities as a result - [please get in touch!](docs/contributing.md) 
In future, we may then embed the best method(s) for this dataset to the `clim-recal` pipeline, following expert review of the data assessments. 

For a quick start on bias correction, refer to our [pipeline guide](python/README.md).

# Documentation

We are in the process of developing comprehensive documentation for our code base to supplement the guidance provided in this and other `README.md` files. In the interim, there is documentation available in the following forms:

## User documentation

- See [setup instructions](setup-instructions.md)
- See `python` [`README`](python/README.md) for an overview of the pipeline
- Once installed, using the `clim-recal --help` option for details
- See the [reproducibility page](docs/reproducibility.qmd) for information on how we used `clim-recal`

## To use `clim-recal` programmatically

- There are extensive [`API Reference`](docs/reference) within the python code.

## To contribute to `clim-recal`

- See the [Contributing](docs/contributing.md) section below
- To contribute to our sister project on assessing bias correction methods for these data, please contact Ruth Bowyer. 


# The Datasets

## UKCP18-CPM
The [UK Climate Projections 2018 (UKCP18)](https://www.metoffice.gov.uk/research/approach/collaboration/ukcp) dataset offers insights into the potential climate changes in the UK. UKCP18 is an advancement of the UKCP09 projections and delivers the latest evaluations of the UK's possible climate alterations in land and marine regions throughout the 21st century. This crucial information aids in future Climate Change Risk Assessments and supports the UK’s adaptation to climate change challenges and opportunities as per the National Adaptation Programme.
We make use of the [Convection Permitting Model (CPM)](https://www.metoffice.gov.uk/pub/data/weather/uk/ukcp18/science-reports/UKCP-Convection-permitting-model-projections-report.pdf). This dataset represents a much finer resolution of climate model (2.2km grid) than typical climate-models, representing a step forward in the ability to simulate small scale behavior (in particular 'atmospheric convection'), and the influence of mountains, coastlines and urban areas. As a result, the CPM provides access to credible climate information important for small-scale weather features and also on local (kilometre) scale; which is particularly important for improving our understanding of climate change in cities. 

## HADS
[HadUK-Grid](https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid/haduk-grid) is a comprehensive collection of climate data for the UK, compiled from various land surface observations across the country. This data is organized into a uniform grid to ensure consistent coverage throughout the UK at up to 1km x 1km resolution. The dataset, spanning from 1836 to the present, includes a variety of climate variables such as air temperature, precipitation, sunshine, and wind speed, available on daily, monthly, seasonal, and annual timescales.

# Why Bias Correction?

Regional climate models contain systematic errors, or biases in their output [^1]. Biases arise in RCMs for a number of reasons, such as the assumptions in the general circulation models (GCMs), and in the downscaling process from GCM to RCM.

Researchers, policy-makers and other stakeholders wishing to use publicly available RCMs need to consider a range of "bias correction” methods (sometimes referred to as "bias adjustment" or "recalibration"). Bias correction methods offer a means of adjusting the outputs of RCM in a manner that might better reflect future climate change signals whilst preserving the natural and internal variability of climate [^2]. The `clim-recal` pipeline provides preprocessed data, including the innovative [UKCP18-CPM datasets](# The Datasets), to faciliate the assessment of these methods without requiring the whole (very large) dataset. 

Our work is however, just like climate data, intended to be dynamic, and we are in the process of setting up a pipeline for researchers creating new methods of bias correction to be able to submit their methods for inclusion on in the `clim-recal` repository.

[^1]: Senatore et al., 2022, <https://doi.org/10.1016/j.ejrh.2022.101120>
[^2]: Ayar et al., 2021, <https://doi.org/10.1038/s41598-021-82715-1>


# Contributing

If you have suggestions on the repository, or would like to include a new method (see below) or library, please
- raise an [issue](https://github.com/alan-turing-institute/clim-recal/issues)
- [get in touch](mailto:clim-recal@turing.ac.uk)
- see our [contributing](docs/contributing.md) section, which includes details on contriubting to the documentation.

All are welcome and appreciated.

# Future plans
- **Adding in bias correction method to pipeline** - following our [sister project](https://github.com/Urban-Analytics-Technology-Platform/bias-correction-for-cpm) reviewing bias correction methods applied to this dataset, we may incorporate the selected method(s) to the pipeline. 

## Acknowledgements

Prior to 12th September 2024 we included a reference to the [python-cmethods](https://github.com/btschwertfeger/python-cmethods) library, written by Benjamin Thomas Schwertfeger.

This was via a git submodule which targeted https://github.com/alan-turing-institute/python-cmethods, itself a fork of the original library.

Inadvertently, we did not identify that the license for the `python-cmethods` library (GPL3) is not compatible with the license for this package (MIT). We apologise for this mistake and have taken the following actions to resolve it:

* We have now removed the relevant submodule from this repository.
* Added this note to the README.
* Added a note to the `python/README.md` file.
* Added the citation below.


## Citation

**python-cmethods**: Benjamin T. Schwertfeger. (2024). btschwertfeger/python-cmethods: v2.3.0 (v2.3.0). Zenodo. https://doi.org/10.5281/zenodo.12168002
