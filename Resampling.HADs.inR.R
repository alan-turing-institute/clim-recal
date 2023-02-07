#### Resampling the HADs 
## As the UKCP18 2.2km historic projections (needed for recal) just run from 1980-2000, regridding these daily observations from the 1km available via HADs
## Hads data from CEDA, Readme file for the data available in vmfileshare/ClimateData/HadUKData
## HADs data is on a 1 km grid OSGB projection (so should be the same as teh reprojected UKCD files )

library(terra)
library(sp)

setwd("/Volumes/vmfileshare/ClimateData/")

#data directory
dd <- getwd()

#list of dds for each HadsUKgrid variable 
v <- c("tasmax","rainfall")
vd <- paste0(dd,"/Raw/HadsUKgrid/",v,"/day/")


HADs.files<- unlist(lapply(vd,list.files))

HADs.nc.files <- HADs.files[grepl("*.nc$", HADs.files)]  

##For recalibration, only need years 1980-2000 
s <- paste0("day_",1981:2000, collapse="|")
HADs.nc.files.slice1 <- HADs.nc.files[grepl(s, HADs.nc.files)] 

#Nested list of variables by terra rasts - One SpatRaster for each year 

List.Rast <- lapply(v, function(x){
  v.files <- HADs.nc.files.slice1[grepl(x, HADs.nc.files.slice1)]
  v.files.d <- paste0(dd,"/Raw/HadsUKgrid/",x,"/day/",v.files)
  HADbrickL <- lapply(v.files.d, rast) #is a list of 240 bricks
  ##Each brick represents a month, with each raster layer within each brick representing the Hads grid for a day
  })


n.names <- paste0("HADbrickL_", v)
names(List.Rast) <- n.names

#### Create Grid from UKCP data 

#Extract the 2.2km grid from the UK raster and then overlay that and create the mean from HADs
## Random ukcp file for this 
f <- "/Volumes/vmfileshare/ClimateData/Processed/UKCP2.2_Reproj/tasmax_bng2/01/latest/tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130.tif"
r <- rast(f)
r <- r$`tasmax_rcp85_land-cpm_uk_2.2km_01_day_19801201-19811130_1`

#Resample -- Transfer values of a SpatRaster to another one with a different geometry

#save.image("~/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/CLIM-RECAL/ResamplingHADS.03.02.22.RData")

Resampled.HADs<- lapply(List.Rast, function(L){
  lapply(L, function(x){
    #Bilinear resampling appropriate for continious vars 
  terra::resample(x, r, method="bilinear", threads=TRUE)
    })
  })
beepr::beep(sound=6)

### Trim the ocean - this has been resampled as 1e + 20, when I assumed it would be NA?
### Should at least be able to trim based on this however!

### Save output
