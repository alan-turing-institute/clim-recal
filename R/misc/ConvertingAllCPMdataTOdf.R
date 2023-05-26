# Script for converting all UKCP CPM input data to dataframes
# Just running for now on the existing projected data 

library(terra)
library(sp)
library(tidyverse)
library(doParallel)
library(doSNOW)
library(foreach)

dd <- "/Volumes/vmfileshare/ClimateData/"

# Data is massive so running in parallel
#To load objects in nodes as spatrasters cannot be serialised - see issue here: https://github.com/rspatial/terra/issues/36

Runs <- c("01", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "15")

file.paths <- lapply(Runs, function(i){
  fp <- paste0(dd, "Reprojected/UKCP2.2/tasmax/", i, "/latest/")
  f <- list.files(fp)
  files <- f[!grepl(".aux.xml", f)]
  files.p <- paste0(fp, files)
})


for(x in 1:12){

  cores <- detectCores()
  cl <- makeCluster(cores[1]-1)
  registerDoSNOW(cl)

  Runpaths <- file.paths[[x]] #Subset to run paths
  i <- 1:length(Runpaths)

  Run.dfs <- foreach(i = i, 
                   .packages = c("terra"),
                   .errorhandling = 'pass') %dopar% {
                     p <- Runpaths[[i]] 
                     r <- rast(p)
                     rdf <- as.data.frame(r) 
                     return(rdf)
                   }

  stopCluster(cl) 

  fn <- paste0("Run.i.",x,"as.df_check.RDS")
  saveRDS(Run.dfs, file=fn)

 remove(Run.dfs)
 gc()
}
