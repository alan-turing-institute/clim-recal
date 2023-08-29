## Crop CPM and HADs 

rm(list=ls())
#setwd("~/Desktop/clim-recal/clim-recal/")
#setwd("/home/dyme/Desktop/clim-recal/clim-recal")
source("R/misc/read_crop.fn.R")

library(tidyverse)
library(data.table)
library(qmap)
library(terra)

dd <- "/mnt/vmfileshare/ClimateData/"

## Using the extents (ie full grid) rather than masking and cropping to city outlines

# 1. London 
f <- paste0(dd,'shapefiles/NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom_2022_7279368953270783580/NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom.shp')
UK.shape <-vect(f)
London <- UK.shape[which(UK.shape$nuts118cd=="UKI")]

fn <- paste0(dd, 'shapefiles/three.cities/London/London.shp')
writeVector(London, fn, overwrite=TRUE)

London.ext <- ext(London)


# 2. Manchester
f <- paste0(dd, 'shapefiles/Major_Towns_and_Cities_Dec_2015_Boundaries_V2_2022_-2549004559791541639/TCITY_2015_EW_BGG_V2.shp')
Cities <- vect(f)
table(Cities$TCITY15NM)
Manchester <- Cities[which(Cities$TCITY15NM=="Manchester")]

fn <- paste0(dd, 'shapefiles/three.cities/Manchester/Manchester.shp')
writeVector(Manchester, fn, overwrite=TRUE)

Manchester.ext <- ext(Manchester)


# 3.Glasgow
f <- paste0(dd, 'shapefiles/Localities2020boundaries/Localities2020_MHW.shp')
Localities <- vect(f)
Glasgow <- Localities[which(Localities$name=="Glasgow")]

fn <- paste0(dd, 'shapefiles/three.cities/Glasgow/Glasgow.shp')
writeVector(Glasgow, fn, overwrite=TRUE)

Glasgow.ext <- ext(Glasgow)

### Apply to the CPM
runs <- c("05", "07", "08", "06")
var <- c("tasmax", "tasmin","pr")

cities <- c("London", "Glasgow", "Manchester")

ext.L <- list(London.ext, Glasgow.ext, Manchester.ext)
names(ext.L) <- cities

lapply(cities, function(x){

      cpm_read_crop(runs=runs, var = var, 
                          fp =  paste0(dd, "Reprojected_infill/UKCP2.2/"),
                          rd = paste0(dd, "Cropped/three.cities/CPM/"),
                          crop.area=ext.L[[x]],
                          cropname=x) })



#### HADS - original 360

var <- c("tasmax", "tasmin", "rainfall")

lapply(cities, function(x){
  
    hads_read_crop(var = var, 
               fp= paste0(dd,  "Processed/HadsUKgrid/resampled_2.2km/"), 
               rd= paste0(dd, "Cropped/three.cities/Hads.original360/"),
               file.date="19801201", #Start from the same date as the CPM
               crop.area=ext.L[[x]],
               cropname=x) })


#### HADs - updated 360 calendar (to be run pending updated files)

var <- c("tasmax", "tasmin", "rainfall")

lapply(cities, function(x){
  
  hads_read_crop(var = var, 
                 fp= paste0(dd,  "Processed/HadsUKgrid/resampled_calendarfix/"), 
                 rd= paste0(dd, "Cropped/three.cities/Hads.updated360/"),
                 file.date="19801201", #Start from the same date as the CPM
                 crop.area=ext.L[[x]],
                 cropname=x) })


