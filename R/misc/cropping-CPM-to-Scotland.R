#### Cropping 'Raw' (reprojected CPM) data to Scotland 
rm(list=ls())

#libs
library(terra)
library(sp)
library(exactextractr)

#data directory
dd <- "/Volumes/vmfileshare/ClimateData/"

# Read in as list of quantile mapping data
## Each decade is seperated out - but potential should we brick these together?
p <- paste0(dd, "Reprojected/UKCP2.2/tasmax/01/latest/")
files <- list.files(p)

raw.files <- files[!grepl("aux.xml", files)]
raw.files.p <- paste0(p, raw.files)
raw.dat <- lapply(raw.files.p, rast) 

raw.dat.r <- rast(raw.dat)
nlyr(raw.dat.r)

#Crop to Scotland extent (for method see Scotland.bbox.R)
scot <- vect("~/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/CLIM-RECAL/clim-recal/data/Scotland/Scotland.bbox.shp")

raw.dat_c <- lapply(raw.dat.r, function(i){ crop(i, scot) })
names(raw.dat_c) <- paste0(names(raw.dat.r), "_cropped")

rd <- "/Volumes/vmfileshare/ClimateData/Interim/Cropped_UKCPM/tasmax/01/"
rd <- "~/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/tempdata/"
n <- names(raw.dat_c)

lapply(n, function(i){
  rast <- raw.dat_c[[i]]
  fn <- paste0(rd,  i, ".tif")
  writeRaster(rast, filename=fn)
})

