## Create a bounding box for Scotland  

library(sf)
library(terra) 
library(raster)

setwd("~/Desktop/clim-recal/clim-recal/data2")

dd <- "/mnt/vmfileshare/ClimateData"

polygon <- vect(paste0(dd,'/shapefiles/infuse_ctry_2011_clipped/infuse_ctry_2011_clipped.shp'))

##### Crop test exent 
Scotland <- polygon[grepl("Scotland", polygon$geo_label), ]

#Extract the extent of Cornwall and create a simple bounding box for extracting climate data 
e <- ext(Scotland)
e2 <- as.polygons(e)
terra::writeVector(e2, "Scotland.bbox.shp")
