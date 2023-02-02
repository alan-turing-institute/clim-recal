# clim-recal

## About
Open repository of methods for recalibrating and bias correcting UKCP18 climate projections data

Our working table of methods is [here](https://docs.google.com/spreadsheets/d/18LIc8omSMTzOWM60aFNv1EZUl1qQN_DG8HFy1_0NdWk/edit?usp=sharing) for now 

## Setup 
Methods can be used with a custom environment, here we provide a Anaconda
environment file for ease-of-use. 
```
conda env create -f environment.yml
```

## Contributing 

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

