#### FOR USE IN CROPPING RASTERS TO THE THREE CITIES 


# A function to read in specific runs, vars and years, crop them to an area (optionally) and write vals to a georef'd df

cpm_read_crop <- function(runs, #Character vector of selected runs as number only eg Run08 is "08"
                          var, #Character vector of selected variables - this might need changing
                          fp, #filepath of parent d of folders where files are - eg paste0(dd, "Reprojected_infill/UKCP2.2/")
                          rd, #path to results directory eg paste0(dd, "Cropped/three.cities/CPM/")
                          crop.area, #Polygon of area to crop to - any Spat obj accepted by terra::crop will work
                          cropname){ #Character - name of crop to be assigned to the returned vect
  
  
  runs <- runs 
  var <- var
  fp <- fp
  rd <- rd
  
  bbox <- crop.area
  cropname <- cropname
  
  for(i in runs){
    for(v in var){
      p <- paste0(fp, v, "/", i, "/latest/")
      files <- list.files(p)
      files <- files[!grepl("aux.xml", files)]
      
      files.p <- paste0(p, files)
      
      # Load and convert remaining to single col dfs 
      dfL <- lapply(1:length(files.p), function(n){
        f <- files.p[[n]] 
        r <- rast(f)
        r_c <- crop(r, bbox, snap="out")
        
        #Write 
        f <- files[[n]]#filename as it was read in 
        fn <- paste0(rd, cropname, "/" , f) 
        
        writeRaster(r_c, fn, overwrite=TRUE) 
        
      }) 
      
      gc()
    }
  }
} 


# HADs function 

hads_read_crop <- function(var, #Character vector of selected variables - this might need changing
                           fp, #filepath of parent d of folders where files are - eg paste0(dd, "Reprojected_infill/UKCP2.2/")
                           rd, #path to results directory eg paste0(dd, "Cropped/three.cities/CPM/")
                           file.date, #Character, Date of HADs file to crop from in YYYYMMDD                                
                           crop.area, #Polygon of area to crop to - any Spat obj accepted by terra::crop will work
                           cropname){ #Character - name of crop to be assigned to the returned vect
  
  var <- var
  fp <- fp
  bbox <- crop.area
  cropname <- cropname
  file.date <- file.date
  
  for(v in var){
    
    HADs.files <- list.files(paste0(fp, v,"/day/"))
    files <- HADs.files[grepl(v, HADs.files)]
    file.i <- grep(file.date,files)
    files <- files[file.i:length(files)]
    files.p <- paste0(fp,  v,"/day/",files)
    
    
    # Load and convert remaining to single col dfs 
    dfL <- lapply(1:length(files.p), function(n){
      f <- files.p[[n]] 
      r <- rast(f)
      r_c <- crop(r, bbox, snap="out")
      
      #Write 
      f <- files[[n]]#filename as it was read in 
      fn <- paste0(rd, cropname, "/" , f) 
      
      writeCDF(r_c, fn, overwrite=TRUE) 
    })
    gc()
  }  
}  

#Function to read and crop and convert to df, but not write it. - at some point should be merged with other R folder

hads_read_crop_df <- function(var, #Character vector of selected variables - this might need changing
                           fp, #filepath of parent d of folders where files are - eg paste0(dd, "Reprojected_infill/UKCP2.2/"). Include up until var folder
                           file.date1, #Character, Date of HADs file to crop from in YYYYMMDD   
                           file.date2, #Character,  Date of HADs file to crop to in YYYYMMDD   
                           crop.area){ #Polygon of area to crop to - any Spat obj accepted by terra::crop will work

  
  var <- var
  fp <- fp
  bbox <- crop.area
  file.date1 <- file.date1
  file.date2 <- file.date2
  
  for(v in var){
    
    HADs.files <- list.files(paste0(fp, v,"/day/"))
    files <- HADs.files[grepl(v, HADs.files)]
    file.i <- grep(file.date1,files)
    file.ii <- grep(file.date2,files)
    files <- files[file.i:file.ii]
    files.p <- paste0(fp,  v,"/day/",files)
    
    # Read in 1st runpath as df with xy coords to ensure overlay with CPM data 
    p <- files.p[[1]] 
    r <- rast(p)
    rdf1 <- as.data.frame(r, xy=T) 
    
    #To ensure subset dataframe has useful naming convention - this does not pull it through as such
    n <- substr(p, nchar(p)-20, nchar(p))
    n <- gsub(".nc","", n)
    names(rdf1) <- gsub("_", paste0(n, "_"), names(rdf1))
    
    # Load and convert remaining to single col dfs
    i <- 2:length(files.p)
    
    dfL <-lapply(i, function(i){
      p <- files.p[[i]] 
      r <- rast(p)
      rdf <- as.data.frame(r)
      #To ensure subset dataframe has useful naming convention - this does not pull it through as such
      n <- substr(p, nchar(p)-20, nchar(p))
      n <- gsub(".nc","", n)
      names(rdf) <- gsub("_", paste0(n, "_"), names(rdf))
      return(rdf)
    }) 
    
    df <- dfL %>% reduce(cbind)
    df <- cbind(rdf1, df)
    
    gc()
  }  
}  



## This function for the different file structure of the updated 360 calendar - to be updated when have confirmation about the files 
hads_read_crop2 <- function(var, #Character vector of selected variables - this might need changing
                            fp, #filepath of parent d of folders where files are - eg paste0(dd, "Reprojected_infill/UKCP2.2/")
                            rd, #path to results directory eg paste0(dd, "Cropped/three.cities/CPM/")
                            file.date, #Character, Date of HADs file to crop from in YYYYMMDD                                
                            crop.area, #Polygon of area to crop to - any Spat obj accepted by terra::crop will work
                            cropname){ #Character - name of crop to be assigned to the returned vect
  
  var <- var
  fp <- fp
  bbox <- crop.area
  cropname <- cropname
  file.date <- file.date
  
  for(v in var){
    
    HADs.files <- list.files(paste0(fp))
    files <- HADs.files[grepl(v, HADs.files)]
    file.i <- grep(file.date,files)
    files <- files[file.i:length(files)]
    files.p <- paste0(fp,  files)
    
    
    # Load and convert remaining to single col dfs 
    dfL <- lapply(1:length(files.p), function(n){
      f <- files.p[[n]] 
      r <- rast(f)
      r_c <- crop(r, bbox, snap="out")
      
      #Write 
      f <- files[[n]]#filename as it was read in 
      fn <- paste0(rd, cropname, "/" , f) 
      
      writeCDF(r_c, fn, overwrite=TRUE) 
    })
    gc()
  }  
}  
