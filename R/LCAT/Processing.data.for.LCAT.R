
source("/home/dyme/Desktop/clim-recal/clim-recal/R/bias-correction-methods/apply_qmapQuant_to_crpd_df_fn.R")

library(terra)
library(tidyverse)
library(data.table)
library(qmap)

Region.Refs <- read.csv("R/LCAT/Region.Refs.csv")
Regioncds <- Region.Refs$Regioncd

#Scotland (UKM) needs to be broked down, so running on everyone else
Regioncds.2 <- Regioncds[c(1:10, 12)]

lapply(Regioncds.2, function(i){
    apply_bias_correction_to_cropped_df(region=i, 
                                    var=c("tasmin", "tasmax", "pr"),
                                    Runs=c("Run05", "Run06", "Run07", "Run08"))})
