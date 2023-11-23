# Script for converting all UKCP CPM input data to dataframes
# Updating to include infill data:
# This script edited from: 'ConvertingallCPMdatatodf.R'
## As a note for future, I did try and run this to extract the means via terra but interestingly would have taken much longer!

library(terra)
library(sp)
library(tidyverse)
library(doParallel)
library(doSNOW)
library(foreach)

dd <- "/Volumes/vmfileshare/ClimateData/"

# Data is massive so running in parallel
#To load objects in nodes as spatrasters cannot be serialised - see issue here: https://github.com/rspatial/terra/issues/36

Runs <- c("01", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "15")

infill.years <- c(2000:2020, 2040:2060, 2080:2100)
# Adding _day_ for clean pulling out of these rasters
infill.years <- paste0("_day_", infill.years, collapse = "|")

file.paths <- lapply(Runs, function(i){
  fp <- paste0(dd, "Reprojected_infill/UKCP2.2/tasmax/", i, "/latest/")
  f <- list.files(fp)
  files <- f[!grepl(".aux.xml", f)]
  # Data for infill only pulled out - if re-run use all
  files <- files[grepl(infill.years, files)]
  files.p <- paste0(fp, files)
})
