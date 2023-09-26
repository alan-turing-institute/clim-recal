## R pre-processing - Converting to dataframe
# Most input to bias correction methods in R need dfs 

# Might be better in future to seperate it out differently (ie not run hads and cpm in same loop, or by variable )

dd <- "/mnt/vmfileshare/ClimateData/"
run <- c("05", "06", "07", "08")
var <- c("tasmax", "tasmin", "pr")

x <- c("London", "Manchester", "Glasgow")

# Where to write the results (note subfolder added as name of city/x above)
rd <- c(paste0(dd, "Interim/CPM/Data_as_df/"), paste0(dd, "Interim/Hads.updated360/Data_as_df/"))

#file locations for cropped versions of CPM and updated hads data
fps <- c(paste0(dd, "Cropped/three.cities/CPM/"),
         paste0(dd, "Cropped/three.cities/Hads.updated360/"))

fps <- paste0(fps, rep(x, 2))

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
      if(v =="pr"){
      for(r in run){
          
        files.paths <- files.paths.all[grepl(v, files.paths.all)& grepl(r, files.paths.all)&grepl("CPM", files.paths.all)]
        
          # Read in 1st runpath as df with xy coords to ensure overlay 
          p1 <- files.paths[[1]] 
          r <- rast(p1)
          rdf1 <- as.data.frame(r, xy=T) 
          
          # Load and convert remaining to single col dfs 
          dfL <- lapply(2:length(file.paths), function(i){
            p <- files.paths[[i]] 
            r <- rast(p)
            rdf <- as.data.frame(r) 
            return(rdf)
          }) 
          
          df <- dfL %>% reduce(cbind)
          df <- cbind(rdf1, df)
          
          rd1 <- rd[grepl("CPM", rd)]
          fn <- paste0(rd1, x, "/", v, "_","Run",r, ".csv")
          write.csv(df, fn, row.names = F)
          
          #Hads
          files.paths <- files.paths.all[grepl(v, files.paths.all)&grepl("Hads", files.paths.all)]
          
          # Read in 1st runpath as df with xy coords to ensure overlay 
          p1 <- files.paths[[1]] 
          r <- rast(p1)
          rdf1 <- as.data.frame(r, xy=T) 
          
          # Load and convert remaining to single col dfs 
          dfL <- lapply(2:length(file.paths), function(i){
            p <- files.paths[[i]] 
            r <- rast(p)
            rdf <- as.data.frame(r) 
            return(rdf)
          }) 
          
          df <- dfL %>% reduce(cbind)
          df <- cbind(rdf1, df)
          
          rd2 <- rd[grepl("Hads", rd)]
          fn <- paste0(rd2, x, "/", v, ".csv")
          write.csv(df, fn, row.names = F)
          
          
      } else{ #Sep run for where rainfall being called as different name in hads
        for(r in run){
          
          files.paths <- files.paths.all[grepl(v, files.paths.all)& grepl(r, files.paths.all)&grepl("CPM", files.paths.all)]
          
          # Read in 1st runpath as df with xy coords to ensure overlay 
          p1 <- files.paths[[1]] 
          r <- rast(p1)
          rdf1 <- as.data.frame(r, xy=T) 
          
          # Load and convert remaining to single col dfs 
          dfL <- lapply(2:length(file.paths), function(i){
            p <- files.paths[[i]] 
            r <- rast(p)
            rdf <- as.data.frame(r) 
            return(rdf)
          }) 
          
          df <- dfL %>% reduce(cbind)
          df <- cbind(rdf1, df)
          
          rd1 <- rd[grepl("CPM", rd)]
          fn <- paste0(rd1, x, "/", v, "_","Run",r, ".csv")
          write.csv(df, fn, row.names = F)
          
          #Hads
          files.paths <- files.paths.all[grepl("rainfall", files.paths.all)&grepl("Hads", files.paths.all)]
          
          # Read in 1st runpath as df with xy coords to ensure overlay 
          p1 <- files.paths[[1]] 
          r <- rast(p1)
          rdf1 <- as.data.frame(r, xy=T) 
          
          # Load and convert remaining to single col dfs 
          dfL <- lapply(2:length(file.paths), function(i){
            p <- files.paths[[i]] 
            r <- rast(p)
            rdf <- as.data.frame(r) 
            return(rdf)
          }) 
          
          df <- dfL %>% reduce(cbind)
          df <- cbind(rdf1, df)
          
          rd2 <- rd[grepl("Hads", rd)]
          fn <- paste0(rd2, x, "/", v, ".csv")
          write.csv(df, fn, row.names = F)
          
          
        } 
      }
    }
          


stopCluster(cl) 
gc()

