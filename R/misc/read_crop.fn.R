#### FOR USE IN CROPPING RASTERS TO THE THREE CITIES 


# A function to read in specific runs, vars and years, crop them to an area (optionally) and write vals to a georef'd df

cpm_read_crop <- function(runs, #Character vector of selected runs as number only eg Run08 is "08"
                               var, #Character vector of selected variables - this might need changing
                               fp, #filepath of parent d of folders where files are - eg paste0(dd, "Reprojected_infill/UKCP2.2/")
                               year1, #Numeric, first year of segment
                               year2, #Numeric, lastyear of segment
                               crop.area, #Polygon of area to crop to - any Spat obj accepted by terra::crop will work
                               cropname){ #Character - name of crop to be assigned to the returned vect
                              
  
  runs <- runs 
  var <- var
  fp <- fp
  years <- paste0(year1:year2, "1201", collapse="|")

    bbox <- crop.area
    
    for(i in runs){
      for(v in var){
        p <- paste0(fp, v, "/", i, "/latest/")
        files <- list.files(p)
        files <- files[!grepl("aux.xml", files)]
        
        files.y <- files[grepl(years, files)]# Historical timeslice 2 for calibration
        files.y.p <- paste0(p, files.y)
        
        # Load and convert remaining to single col dfs 
        dfL <- lapply(1:length(files.y.p), function(n){
          f <- files.y.p[[n]] 
          r <- rast(f)
          r_c <- crop(r, bbox, snap="out")
          }) 
        
        R <- dfL %>% reduce(c)
        
        #Write directory
        rp <- paste0(dd, "Interim/CPM/three.cities/", cropname, "/" , cropname,"_") #adding in cropname to write, I think will make easier to track
        
        fn <- paste0(rp, v, "_rcp85_land-cpm_uk_2.2km_", i, "_day_", year1, "_", year2, ".tif")
        writeRaster(R, fn, overwrite=TRUE) 
        
        gc()
      }
    }
} 


# HADs function 

hads_read_crop <- function(var, #Character vector of selected variables - this might need changing
                                   fp, #filepath of parent d of folders where files are - eg paste0(dd, "Reprojected_infill/UKCP2.2/")
                                   i1, ## First file n index, eg for 1980-2010 this is files [1:360] i1=1 (I appreciate this is a lazy code)
                                   i2, ## First file n index, eg for 1980-2010 this is files [1:360] i2=360 (I appreciate this is a lazy code)
                                   crop.area, #Polygon of area to crop to - any Spat obj accepted by terra::crop will work
                                   cropname){ #Character - name of crop to be assigned to the returned df - usually the crop area

  var <- var
  fp <- fp
  bbox <- crop.area
  cropname <- cropname

  for(v in var){
    
    HADs.files <- list.files(paste0(fp, v,"/day/"))
    files <- HADs.files[grepl(v, HADs.files)]
    Runpaths <- paste0(fp,v,"/day/",files[i1:i2]) 
    
      # Load and convert remaining to single col dfs
      i <- 1:length(Runpaths)
    
      dfL <-lapply(i, function(i){
        p <- Runpaths[[i]] 
        r <- rast(p)
        r_c <- crop(r, bbox, snap="out")}) 
      
      R <- dfL %>% reduce(c)
      

      #To ensure each layer has a useful naming convention

      lyr.n <-unlist(lapply(i, function(i){
      p <- Runpaths[[i]]   
      rast.names <- names(rast(p))
      
      n <- substr(p, nchar(p)-20, nchar(p))
      n <- gsub(".nc","", n)
      nn <- paste0("hadukgrid_2.2km_resampled", n, "_", rast.names)}))
      
      names(R) <- lyr.n
      
      #Write directory
      rp <- paste0(dd, "Interim/HadsUK/three.cities/", cropname, "/" , cropname,"_") #adding in cropname to write, I think will make easier to track
      
      fn1 <- Runpaths[[1]]
      fn1 <- gsub(".*resampled_", "",fn1)
      fn1 <- gsub("-.*", "", fn1)
      
      ii <- length(Runpaths)
      fn2 <- Runpaths[[ii]]
      fn2 <- gsub(".*resampled_", "",fn2)
      fn2 <- gsub(".*-", "", fn2)
      fn2 <- gsub(".nc", "", fn2)
      
      fn <- paste0(rp, v, "_hadukgrid_2.2km_resampled_",fn1, "_", fn2, ".tif")
      writeRaster(R, fn, overwrite=TRUE) 
      
      gc()
    
  }  
}  
