rm(list=ls())

#setwd("~/Desktop/clim-recal/clim-recal/")
setwd("/home/dyme/Desktop/clim-recal/clim-recal")
source("R/LCAT/LCATv_apply_qmapQuant_to_crpd_df_fn.R")

library(terra)
library(tidyverse)
library(data.table)
library(qmap)

Region.Refs <- read.csv("R/bias-correction-methods/R/LCAT/Region.Refs.csv")
Regioncds <- Region.Refs$Regioncd

#Scotland (UKM) needs to be broken down, so running on everyone else
Regioncds.2 <- Regioncds[c(1:10, 12)] 

  apply_bias_correction_to_cropped_df(region=Regionscds.2, 
                                      var=c("tasmin", "tasmax", "pr"),
                                      Runs=c("Run05", "Run06", "Run07", "Run08"))

## Scotland -- further cropping so as to proccess 
  cropdf_further_apply_bc_to_cropped_df(region = "UKM", #Region code - needs to relate to the file name in a unique way to subset
                                        var=c("tasmax"),
                                        Runs=c("Run06"), 
                                        N.new.segments=4)
  

