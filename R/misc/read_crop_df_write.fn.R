
write.csv.date <- function(x, y){
  date <- Sys.Date()
  date <- gsub("-", ".", date)
  fn <- y
  rd <- rd
  csvFileName <- paste(rd,"/",fn,".",date,".csv",sep="")
  write.csv(x, file=csvFileName, row.names = F)}

# A function to read in specific runs, vars and years 

cpm_read_crop_df_write <- function(runs, #Character vector of selected runs
                               var, #Character vector of selected variables - this might need changing
                               fp, #filepath of parent d of folders where files are - eg paste0(dd, "Reprojected_infill/UKCP2.2/")
                               year1, #Numeric, first year of segment
                               year2, #Numeric, lastyear of segment
                               name1, #Character - first part of name to be assigned to the returned df- usually the model
                               crop, #logical
                               crop.area, #Polygon of area to crop to - any Spat obj accepted by terra::crop will work
                               cropname, #Character - name of crop to be assigned to the returned df - usually the crop area
                               rd){ # results directory for storing results
  
  runs <- runs 
  var <- var
  years <- paste0(year1:year2, "1201", collapse="|")

  if(crop == T){
    
    bbox <- crop.area
    
    for(i in runs){
      for(v in var){
        p <- paste0(fp, v, "/", i, "/latest/")
        files <- list.files(p)
        files <- files[!grepl("aux.xml", files)]
        
        files.y <- files[grepl(years, files)]# Historical timeslice 2 for calibration
        files.y.p <- paste0(p, files.y)
        
        # Read in 1st runpath as df with xy coords to ensure overlay 
        p1 <- files.y.p[[1]] 
        r <- rast(p1)
        r_c <- crop(r, bbox, snap="out", mask=T) 
        rdf1 <- as.data.frame(r_c, xy=T) 
        
        # Load and convert remaining to single col dfs 
        dfL <- lapply(2:length(files.y.p), function(i){
          p <- files.y.p[[i]] 
          r <- rast(p)
          r_c <- crop(r, bbox, snap="out", mask=T) 
          rdf <- as.data.frame(r_c) 
          return(rdf)
        }) 
        
        df <- dfL %>% reduce(cbind)
        df <- cbind(rdf1, df)
        
        fn <- paste0(name1, "_", cropname, year1, "_", year2, v, "_Run", i)
        
        rd <- rd
        write.csv.date(df, fn)
        gc()
      }
    }
  } else { #for where no crop to be applied
    
    for(i in runs){
      for(v in var){
        p <- paste0(fp, v, "/", i, "/latest/")
        files <- list.files(p)
        files <- files[!grepl("aux.xml", files)]
        
        files.y <- files[grepl(years, files)]# Historical timeslice 2 for calibration
        files.y.p <- paste0(p, files.y)
        
        # Read in 1st runpath as df with xy coords to ensure overlay 
        p1 <- files.y.p[[1]] 
        r <- rast(p1)
        rdf1 <- as.data.frame(r_c, xy=T) 
        
        # Load and convert remaining to single col dfs 
        dfL <- lapply(2:length(files.y.p), function(i){
          p <- files.y.p[[i]] 
          r <- rast(p)
          rdf <- as.data.frame(r_c) 
          return(rdf)
        }) 
        
        df <- dfL %>% reduce(cbind)
        df <- cbind(rdf1, df)
        
        rd <- rd
        
        fn <- paste0(name1, "_", cropname, year1, "_", year2, v, "_Run", i)
        
        write.csv.date(df, fn)
        
        gc()
      }
    }
  }
}


# HADs function 

hads19802010_read_crop_df_write <- function(var, #Character vector of selected variables - this might need changing
                                   fp, #filepath of parent d of folders where files are - eg paste0(dd, "Reprojected_infill/UKCP2.2/")
                                   name1, #Character - first part of name to be assigned to the returned df- usually the model
                                   crop, #logical
                                   crop.area, #Polygon of area to crop to - any Spat obj accepted by terra::crop will work
                                   cropname, #Character - name of crop to be assigned to the returned df - usually the crop area
                                   rd){ # results directory for storing results
  
  var <- var
  fp <- fp
  crop <- crop

  for(v in var){
    
    HADs.files <- list.files(paste0(fp, v,"/day/"))
    files <- HADs.files[grepl(v, HADs.files)]
    Runpaths <- paste0(fp,v,"/day/",files[1:360]) #Subsetting to years 1980-2010 - if we download different data then this would need to be changed
    
    if(crop == TRUE){
      
      bbox <- crop.area
      cropname <- cropname
    
      # Read in 1st runpath as df with xy coords to ensure overlay with CPM data 
       p <- Runpaths[[1]] 
       r <- rast(p)
       r_c <- crop(r, bbox, snap="out", mask=T) 
       rdf1 <- as.data.frame(r_c, xy=T) 
    
      #To ensure subset dataframe has useful naming convention - this does not pull it through as such
      n <- substr(p, nchar(p)-20, nchar(p))
      n <- gsub(".nc","", n)
      names(rdf1) <- gsub("_", paste0(n, "_"), names(rdf1))
    
      # Load and convert remaining to single col dfs
      i <- 2:length(Runpaths)
    
      dfL <-lapply(i, function(i){
        p <- Runpaths[[i]] 
        r <- rast(p)
        r_c <- crop(r, bbox, snap="out", mask=T) 
        rdf <- as.data.frame(r_c)
          #To ensure subset dataframe has useful naming convention - this does not pull it through as such
        n <- substr(p, nchar(p)-20, nchar(p))
        n <- gsub(".nc","", n)
        names(rdf) <- gsub("_", paste0(n, "_"), names(rdf))
      return(rdf)
    }) 
    
    df <- dfL %>% reduce(cbind)
    df <- cbind(rdf1, df)
    
    rd <- rd
    
    fn <- paste0(name1,cropname,"1980_2010_", v)
    
    write.csv.date(df, fn)
    
    gc()
    
    } else {
      
      
      # Read in 1st runpath as df with xy coords to ensure overlay with CPM data 
      p <- Runpaths[[1]] 
      r <- rast(p)
      rdf1 <- as.data.frame(r, xy=T) 
      
      #To ensure subset dataframe has useful naming convention - this does not pull it through as such
      n <- substr(p, nchar(p)-20, nchar(p))
      n <- gsub(".nc","", n)
      names(rdf1) <- gsub("_", paste0(n, "_"), names(rdf1))
      
      # Load and convert remaining to single col dfs
      i <- 2:length(Runpaths)
      
      dfL <-lapply(i, function(i){
        p <- Runpaths[[i]] 
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
      
      rd <- rd
      
      fn <- paste0(name1,"1980_2010_", v)
      
      write.csv.date(df, fn)
      
      gc()
      
    }
  }  
}  
