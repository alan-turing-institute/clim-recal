## Crop CPM and HADs 

rm(list=ls())

source("~/Desktop/clim-recal/clim-recal/R/misc/read_crop.fn.R")

library(tidyverse)
library(data.table)
library(qmap)

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
                          fp = paste0(dd, "Reprojected/UKCP2.2/"),
                          year1=1980,
                          year2=2000,
                          crop.area=ext.L[[x]],
                          cropname=x) })


# Splitting up next time slice for calib and val 

lapply(cities, function(x){
  
  cpm_read_crop(runs=runs, var = var, 
                fp = paste0(dd, "Reprojected_infill/UKCP2.2/"),
                year1=2000,
                year2=2010,
                crop.area=ext.L[[x]],
                cropname=x) })

lapply(cities, function(x){
  
  cpm_read_crop(runs=runs, var = var, 
                fp = paste0(dd, "Reprojected_infill/UKCP2.2/"),
                year1=2010,
                year2=2020,
                crop.area=ext.L[[x]],
                cropname=x) })

## Next time slice 2020-2040
lapply(cities, function(x){
  
  cpm_read_crop(runs=runs, var = var, 
                fp = paste0(dd, "Reprojected/UKCP2.2/"),
                year1=2020,
                year2=2040,
                crop.area=ext.L[[x]],
                cropname=x) })


## Next time slice 2040-2060
lapply(cities, function(x){
  
  cpm_read_crop(runs=runs, var = var, 
                fp = paste0(dd, "Reprojected_infill/UKCP2.2/"),
                year1=2040,
                year2=2060,
                crop.area=ext.L[[x]],
                cropname=x) })

## Next time slice 2060-2080
lapply(cities, function(x){
  
  cpm_read_crop(runs=runs, var = var, 
                fp = paste0(dd, "Reprojected/UKCP2.2/"),
                year1=2060,
                year2=2080,
                crop.area=ext.L[[x]],
                cropname=x) })



#### HADS

#Calibration years files 1 - 360 (first 30 years)

var <- c("tasmax", "tasmin", "rainfall")

lapply(cities, function(x){
  
    hads_read_crop(var = var, 
               fp= paste0(dd,  "Processed/HadsUKgrid/resampled_2.2km/"), 
               i1 = 1, i2 = 360,
               crop.area=ext.L[[x]],
               cropname=x) })

#Validation years files 361 - 480 -- years 2010 - 2020

lapply(cities, function(x){
  
  hads_read_crop(var = var, 
                 fp= paste0(dd,  "Processed/HadsUKgrid/resampled_2.2km/"), 
                 i1 = 361, i2 = 480,
                 crop.area=ext.L[[x]],
                 cropname=x) })


### Group the CPM to cal, val and projection 
runs <- c("05", "07", "08", "06")
var <- c("tasmax", "tasmin","pr")

for(x in cities){
  for(r in runs){
    for(v in var){
    p <- paste0(dd, "Interim/CPM/three.cities/", x, "/")
    files <- list.files(p)
    
    files.y.v <- files[grepl("day_1980|day_2000", files)&grepl(v, files)&grepl(paste0(r, "_day"), files)]
    
    dfL <- lapply(files.y.v, function(n){
      f <- paste0(p, n)
      r <- rast(f)
    }) 
    
    R <- dfL %>% reduce(c)
    
    #Write directory
    rp <- paste0(dd, "Interim/CPM/three.cities/", x, "/grouped/",x, "_") #adding in cropname to write, I think will make easier to track
    
    fn <- paste0(rp, v, "_", r,"_calibration_1980-2010.tif")
    writeRaster(R, fn, overwrite=TRUE) 
    
    gc()
  }
}
} 

#For validation I just copied over and renamed the files as they were already split that way

## Projection years 

for(x in cities){
  for(r in runs){
    for(v in var){
      p <- paste0(dd, "Interim/CPM/three.cities/", x, "/")
      files <- list.files(p)
      
       files.y.v <- files[grepl("day_2020|day_2040|day_2060", files)&grepl(v, files)&grepl(paste0(r, "_day"), files)]
      
      dfL <- lapply(files.y.v, function(n){
        f <- paste0(p, n)
        r <- rast(f)
      }) 
      
      
      R <- dfL %>% reduce(c)
      
      #Write directory
      rp <- paste0(dd, "Interim/CPM/three.cities/", x, "/grouped/",x, "_") #adding in cropname to write, I think will make easier to track
      
      fn <- paste0(rp, v, "_", r,"_projection_2020-2080.tif")
      writeRaster(R, fn, overwrite=TRUE) 
      
      gc()
    }
  }
} 
