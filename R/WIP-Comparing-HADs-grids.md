Comparing-Reprojections-HADs
================

``` r
rm(list=ls())

knitr::opts_knit$set(root.dir="/Volumes/vmfileshare/ClimateData/")

library(terra)
library(sp)
library(exactextractr)

dd <- "/Volumes/vmfileshare/ClimateData/"
```

## **0. About**

Bias correction techniques in general require observational data to
compare with climate projections in order to appropriately correct the
bias.

The [HadUK
grid](https://catalogue.ceda.ac.uk/uuid/bbca3267dc7d4219af484976734c9527)
is a 1km x 1km gridded dataset derived from meterological station
observations.

The first UKCP product for review is the UCKP convection-permitting
dataset, on a 2.2km grid. Therefore, we are resmapling the 1km grid
using bilenear interpolation to 2.2km grid extent.

We have ran this seperately in both `r` and `python`. The aim of this
doc is to:

-   Ensure both methods produce the same result
-   Ensure the grid has been resampled to the correct extent and CRS

## **1. Data**

**1a. HadUK grid resampled in R** Resampling script
[here](https://github.com/alan-turing-institute/clim-recal/blob/main/R/Resampling.HADs.inR.R)
The 2.2km grid was derived from a reprojected (to BNG) UKCP 2.2km .nc
file

In resampling it resampled the Sea as xx so replacing those vals as NA

``` r
r1 <- paste0(dd,"TestData.R-python/Resampled_HADs_tasmax.2000.01.tif")
r1 <- rast(r1)#Contains 31 layers for each day of Jan

#In the resampling, the method used seemed to have relable all Sea values as '1.000000e+20' so relabelling them here (but to be checked as to why they've been valued like this in the resampling)
r1[r1 > 200] = NA

#check the crs
crs(r1, proj=T)
```

    ## [1] "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +a=6377563.396 +rf=299.324961266495 +units=m +no_defs"

``` r
#Plot to check
plot(r1$tasmax_1)
```

![](WIP-Comparing-HADs-grids_files/figure-gfm/load%20data%201-1.png)<!-- -->

**1b. HadUK grid resampled in python** Resampling script
[here](https://github.com/alan-turing-institute/clim-recal/blob/main/python/resampling/resampling_hads.py)

``` r
py.pros.tasmax <- list.files(paste0(dd, "Processed/HadsUKgrid/resampled_2.2km/tasmax/day"))
r2 <- py.pros.tasmax[grepl("200001", py.pros.tasmax)] #Same file as resampled above
r2 <- paste0(paste0(dd, "Processed/HadsUKgrid/resampled_2.2km/tasmax/day"),"/",r2)
r2 <- rast(r2)
crs(r2) #check crs 
```

    ## [1] ""

``` r
## Ok so interesting is missing a crs slot on read - I wonder why this is? This could cause future problem potentially? 

plot(r2$tasmax_1)
```

![](WIP-Comparing-HADs-grids_files/figure-gfm/load%20data%202-1.png)<!-- -->

**1c. Original HADUK grid**

``` r
f <- paste0(dd, "Raw/HadsUKgrid/tasmax/day/")
hads.tasmax <- list.files(f)

hads.tasmax2 <- hads.tasmax[grepl("200001", hads.tasmax )] #Same file as resampled above
og <- paste0(f, hads.tasmax2)

og <- rast(og)
crs(og, proj=T)
```

    ## [1] "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +a=6377563.396 +rf=299.324961266495 +units=m +no_defs"

``` r
plot(og$tasmax_1)
```

![](WIP-Comparing-HADs-grids_files/figure-gfm/unnamed-chunk-1-1.png)<!-- -->

**1d. Cropped extent** Just comparing by cropping to Scotland (bbox
created
[here](https://github.com/alan-turing-institute/clim-recal/tree/main/data/Scotland))

``` r
scotland <- vect("~/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/CLIM-RECAL/clim-recal/data/Scotland/Scotland.bbox.shp")
```

## **2. Comparisons **

Crop extents to be the same

``` r
#Noticed the crop takes longer on r2_c - for investigation!

b <- Sys.time()
r1_c <- terra::crop(r1, scotland, snap="in") 
e <- Sys.time()
e-b
```

    ## Time difference of 0.01999497 secs

``` r
plot(r1_c$tasmax_1)
```

![](WIP-Comparing-HADs-grids_files/figure-gfm/unnamed-chunk-3-1.png)<!-- -->

``` r
b <- Sys.time()
r2_c <- terra::crop(r2, scotland, snap="in") 
e <- Sys.time()
e-b
```

    ## Time difference of 28.5312 secs

``` r
plot(r2_c$tasmax_1)
```

![](WIP-Comparing-HADs-grids_files/figure-gfm/unnamed-chunk-4-1.png)<!-- -->

``` r
og_c <- terra::crop(og, scotland, snap="in") 
plot(og_c$tasmax_1)
```

![](WIP-Comparing-HADs-grids_files/figure-gfm/unnamed-chunk-5-1.png)<!-- -->
Ok there are some differences that I can see from the plot between the
two resampled files!

``` r
## Cropping to a small area to compare with the same orginal HADS file 
i <- rast()
ext(i) <- c(200000, 210000, 700000, 710000)

r1_ci <- crop(r1_c, i)
plot(r1_ci$tasmax_1)
```

![](WIP-Comparing-HADs-grids_files/figure-gfm/unnamed-chunk-6-1.png)<!-- -->

``` r
r2_ci <- crop(r2_c, i)
plot(r2_ci$tasmax_1)
```

![](WIP-Comparing-HADs-grids_files/figure-gfm/unnamed-chunk-7-1.png)<!-- -->

``` r
og_ci <- crop(og_c, i)
plot(og_ci$tasmax_1)
```

![](WIP-Comparing-HADs-grids_files/figure-gfm/unnamed-chunk-8-1.png)<!-- -->

The resampled grids also align differently

I’m going to also pull a UKCP file in here to check
