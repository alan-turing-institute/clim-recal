
source("/home/dyme/Desktop/clim-recal/clim-recal/R/bias-correction-methods/apply_qmapQuant_to_crpd_df_fn.R")

library(terra)
library(tidyverse)
library(data.table)
library(qmap)

Region.Refs.csv

apply_bias_correction_to_cropped_df(region="UKM", 
                                    var=c("tasmin", "tasmax", "pr"),
                                    Runs=c("Run07", "Run08"),
                                    crop_further = TRUE)
