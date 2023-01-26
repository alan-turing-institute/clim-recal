#### Resampling the HADs 
## As the UKCP18 2.2km historic projections (needed for recal) just run from 1980-2000, regridding these daily observations from the 1km available via HADs
## Hads data from CEDA, Readme file for the data available in vmfileshare/ClimateData/HadUKData
## HADs data is on a 1 km grid OSGB projection (so should be the same as teh reprojected UKCD files )

#Libs
library(raster)

setwd("/Volumes/vmfileshare/ClimateData/")

#data directory
dd <- getwd()

#list of dds for each HadsUKgrid variable 
## currently we have tasmax and tasmin
v <- c("tasmax", "tasmin")
vd <- paste0(dd,"/Raw/HadsUKgrid/",v,"/")


HADs.files<- unlist(lapply(vd,list.files))

HADs.nc.files <- HADs.files[grepl("*.nc$", HADs.files)] #744 files 

##For recalibration, only need years 1980-2000 
s <- paste0("day_",1981:2000, collapse="|")
HADs.nc.files.slice1 <- HADs.nc.files[grepl(s, HADs.nc.files)]

#One brick for each year 
a <- Sys.time()
B <- lapply(v, function(x){
  HAD.slice <- HADs.nc.files.slice1[grepl(x, HADs.nc.files.slice1)]
  vd_v <- vd[grepl(x, vd)] 
  HAD.slice.d <- paste0(vd_v, HAD.slice)
  HADbrickL <- lapply(HAD.slice.d, brick)
})
b <- Sys.time()
a - b

#HADbrickL is a list of 240 bricks
#Each brick represents a month, with each raster layer within each brick representing the Hads grid for a day 

#Confirming numbers are correct


#To get the correct extent (ie the sames as above in UKCD, rather than the MSOA group extent) 
# going to first create the grid frame which is used for the reprojection of Hads data, and then use this total extent to crop Hads

#### Create Grid from UKCP data 

#Extract the 2.2km grid from the UK raster and then overlay that and create the mean from HADs

#Using a random cropped raster to the prototypic extent create the grid then average everything on the same long/lats as before
r <- Run4Y1$tasmax_rcp85_land.cpm_uk_2.2km_06_day_19881201.19891130_1
grid <- as(r,'SpatialPolygonsDataFrame')

e2 <- extent(grid@bbox)
e2 <- as(e2,"SpatialPolygons")

#Name the bricks for easier merging
HAD_months <- sapply(1981:2000, function(y){paste0("Hads",y,"_", 01:12)})
names(HADbrickL) <- HAD_months

#Crop HADs as with UKCD files -not necessary if running on whole of UK
HADbrickL_crop <- lapply(HADbrickL, function(x){crop(x, e2, snap="out")})

#Returns list of bricks, where each brick represents the year of daily Hads observation for the calibration period. This is to make it easier later to align to UKCP
HADbrickL_crop_Y <- lapply(1981:2000, function(Y){
  tobrick <- HADbrickL_crop[grepl(paste0("Hads",Y),names(HADbrickL_crop))]
  b <- brick(tobrick)
})

names(HADbrickL_crop_Y)<- paste0("Y", 1981:2000)


#### Resample HADS data to UKCP grid 


#Resample -- Transfer values of a SpatRaster to another one with a different geometry
r <- Run4Y1$tasmax_rcp85_land.cpm_uk_2.2km_06_day_19881201.19891130_1 #Random day as before - just using for the geometry

HADbrickL_crop_Y_2.2km <- lapply(HADbrickL_crop_Y, function(x){
  terra::resample(x, r, method="bilinear")}) #Bilinear resampling appropriate for continious vars 

