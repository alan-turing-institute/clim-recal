## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## 
## Data Processing UK HadsGrid CPM data from raster to data.frame

## Testing if extracting vals is quicker - is hard because storing the rasters is tricky 

## 0. About

# Many of the methods packages for applying bias correction to climate date take as input vector, matrix or data.frame, 
# rather than a spatial file or raster
# This script is not very efficient, but is how I've been converting all the data to data.frame
# Because processing is challenging, I've been creating a 'bbox' based on a area shape file- this allows running cropped to regions


rm(list=ls())

# libs
library(terra)
library(sf)
library(exactextractr)
library(reshape2) #melt
library(tidyverse) #
library(doParallel)

#Loaded package versions
x <- c("MBC", "terra", "sf", "exactextractr")
lapply(x,packageVersion)

#Path is "/<mount location>/vmfileshare/ClimateData
#dd <- "/Volumes/vmfileshare/ClimateData/"
dd <- "/mnt/vmfileshare/ClimateData/"

#Need to define results directory (rd) in global

source("R/misc/read_crop_df_write.fn.R")

## Run cropped to test, then write parallel loop for other variables possibly
f <- paste0(dd,'shapefiles/NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom_2022_7279368953270783580/NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom.shp')
UK.shape <-vect(f)

#Loop over each section of the UK as indicated here
regions <- UK.shape$nuts118nm
regioncd <- UK.shape$nuts118cd

# Run in parallel for 2 regions to check
x <- regioncd[3:length(regioncd)]
rd <- paste0(dd, "Interim/HadsUK/Data_as_df/")

cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)

    foreach(x = x, 
         .packages = c("terra", "tidyverse"),
             .errorhandling = 'pass') %dopar% { 

               f <- paste0(dd,'shapefiles/NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom_2022_7279368953270783580/NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom.shp')
               UK.shape <-vect(f)
               crop.area <- UK.shape[which(UK.shape$nuts118cd==x)]
                       
              var <- c("rainfall", "tasmax", "tasmin", "tas")
              
              hads19802010_read_crop_df_write(var = var,
                                              fp = paste0(dd,  "Processed/HadsUKgrid/resampled_2.2km/"),
                                              name1 = "HadsUK",
                                              crop=T,
                                              crop.area=crop.area,
                                              cropname=x,
                                              rd=rd)
                      
                     }
  
  stopCluster(cl) 
  gc()




