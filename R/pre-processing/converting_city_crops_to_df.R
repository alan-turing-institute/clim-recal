## R pre-processing - Converting to dataframe

rm(list=ls())

# Most input to bias correction methods in R need dfs 

# Might be better in future to seperate it out differently (ie not run hads and cpm in same loop, or by variable )
library(qmap)
library(terra)
library(tidyverse) 
library(doParallel)

dd <- "/mnt/vmfileshare/ClimateData/"
run <- c("05", "06", "07", "08")
var <- c("tasmax", "tasmin", "pr")

x <- c("London", "Manchester", "Glasgow")

# Where to write the results (note subfolder added as name of city/x above)
rd <- paste0(dd, "Interim/CPM/Data_as_df/three.cities/")

#file locations for cropped versions of CPM 
fps <- paste0(dd, "Cropped/three.cities/CPM/")
fps <- paste0(fps,x)

#Run the conversion to df in parallel 
cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)

foreach(x = x, 
        .packages = c("terra", "tidyverse"),
        .errorhandling = 'pass') %dopar% { 
          
          fp <- fps[grepl(x, fps)]
          fp <- paste0(fp, "/")
          files <- list.files(fp)
          files.paths.all <- paste0(fp, files)
          
          #group in runs and in variable
          
    for(v in var){
      for(r in run){
          
        files.paths <- files.paths.all[grepl(v, files.paths.all)& grepl(r, files.paths.all)&grepl("CPM", files.paths.all)]
        
          # Read in 1st runpath as df with xy coords to ensure overlay 
          p1 <- files.paths[[1]] 
          rast <- rast(p1)
          rdf1 <- as.data.frame(rast, xy=T) 
          
          # Load and convert remaining to single col dfs 
          dfL <- lapply(2:length(files.paths), function(i){
            p <- files.paths[[i]] 
            rast <- rast(p)
            rdf <- as.data.frame(rast) 
            return(rdf)
          }) 
          
          df <- dfL %>% reduce(cbind)
          df <- cbind(rdf1, df)
          
          fn <- paste0(rd, x, "/", v, "_","Run",r, ".csv")
          write.csv(df, fn, row.names = F)
          
          
      } 
    }
        }
stopCluster(cl) 
gc()


#HADS
rd <- paste0(dd, "Interim/HadsUK/Data_as_df/three.cities/")

#file locations for cropped versions of HADs
fps <- paste0(dd, "Cropped/three.cities/Hads.updated360/")
fps <- paste0(fps,x)

#Run the conversion to df in parallel 
cores <- detectCores()
cl <- makeCluster(cores[1]-1)
registerDoParallel(cl)

foreach(x = x, 
        .packages = c("terra", "tidyverse"),
        .errorhandling = 'pass') %dopar% { 
          
          fp <- fps[grepl(x, fps)]
          fp <- paste0(fp, "/")
          files <- list.files(fp)
          files.paths.all <- paste0(fp, files)
          
          #group in runs and in variable
          
          for(v in var){
              if(v!="pr"){
              
              files.paths <- files.paths.all[grepl(v, files.paths.all)]
              
              # Read in 1st runpath as df with xy coords to ensure overlay 
              p1 <- files.paths[[1]] 
              rast <- rast(p1)
              rdf1 <- as.data.frame(rast, xy=T) 
              
              # Load and convert remaining to single col dfs 
              dfL <- lapply(2:length(files.paths), function(i){
                p <- files.paths[[i]] 
                rast <- rast(p)
                rdf <- as.data.frame(rast) 
                return(rdf)
              }) 
              
              df <- dfL %>% reduce(cbind)
              df <- cbind(rdf1, df)
              

              fn <- paste0(rd, x, "/", v, ".csv")
              write.csv(df, fn, row.names = F)
              
              
              } else {
                
                files.paths <- files.paths.all[grepl("rainfall", files.paths.all)]
                
                # Read in 1st runpath as df with xy coords to ensure overlay 
                p1 <- files.paths[[1]] 
                rast <- rast(p1)
                rdf1 <- as.data.frame(rast, xy=T) 
                
                # Load and convert remaining to single col dfs 
                dfL <- lapply(2:length(files.paths), function(i){
                  p <- files.paths[[i]] 
                  rast <- rast(p)
                  rdf <- as.data.frame(rast) 
                  return(rdf)
                }) 
                
                df <- dfL %>% reduce(cbind)
                df <- cbind(rdf1, df)
                
                
                fn <- paste0(rd, x, "/", v, ".csv")
                write.csv(df, fn, row.names = F)
                
            }
          }
        }
stopCluster(cl) 
gc()
