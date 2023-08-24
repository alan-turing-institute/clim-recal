#Re-writing WIP_EQM as a loop, to loop over each segment and hopefully create bias corrected for all UK 

##Loading data as created in 'Data_Processing_todf.R' 

#Requires
library(tidyverse)
library(data.table)
library(qmap)


apply_bias_correction_to_cropped_df <- function(region, #Region code - needs to relate to the file name in a unique way to subset
                                                var, #Meterological variables
                                                Runs){

  i <- region   

for(r in Runs){
  for(v in var){
    if(v!="pr"){
      dd <- "/mnt/vmfileshare/ClimateData/"

      #Subset to Area
      #HADs grid observational data
        fp <- paste0(dd, "Interim/HadsUK/Data_as_df/")
        files <- list.files(fp)
        obs <- files[grepl(i, files)]

        #subset file list to var
        obs.var <-  obs[grepl(v,obs)]
        
        #subset to calibration years 
        obs.varc <- obs.var[grepl("1980", obs.var)]
        obs.df <- fread(paste0(fp, obs.varc))
        obs.df <- as.data.frame(obs.df)

        row.names(obs.df) <- paste0(obs.df$x, "_", obs.df$y )
        obs.df$x <- NULL
        obs.df$y <- NULL

        #Remove the dates not in the cpm 
        ## find col position of the first cpm date 19801201
        n1 <-min(grep("19801201", names(obs.df)))
        obs.df <- obs.df[c(n1:ncol(obs.df))]
        
        
        #Using 1980 - 2010 as calibration period
        fp <- paste0(dd, "Interim/CPM/Data_as_df/")
        cpm.files <- list.files(fp)

        #Calibration years 1980 - 2010  - load in full one for 1980 - 2000
        cpm.cal <- cpm.files[grepl("1980|2000", cpm.files)]

        #Subset file list to area 
        cpm.cal <- cpm.cal[grepl(i, cpm.cal)]

        #subset to var and run
        cpm.cal.var <- cpm.cal[grepl(v, cpm.cal)&grepl(r, cpm.cal)]

        #Load in 
        cal.df <- lapply(cpm.cal.var, function(x){
        df <- fread(paste0(fp, x))
        df <- as.data.frame(df)
        
        row.names(df)<- paste0(df$x, "_", df$y)
        df$x <- NULL
        df$y <- NULL
        return(df)
        })
        
        cal.df <- cal.df %>% reduce(cbind)

        #Sub out beyond cal period (2010 - 2020) - ie just keep the calibration here 
        #Keep all of the files with these years - because the naming convention runs 
        #from Nov to following year we need to just take the first 30 days of the one starting with 20091201-
        n2 <- min(grep("20091201-",names(cal.df))) + 29
        
        #This is the first part of the validation dataset, but all the val will be added to the projection df for 
        #the sake of bias correction and assessed separately
        proj.df1 <- cal.df[c((n2+1):ncol(cal.df))]
        cal.df <- cal.df[c(1:n2)]
        
        gc()
  
        yi <- paste0(i,c(2020,2040,2060), collapse="|")
        cpm.proj <- cpm.files[grepl(yi, cpm.files)]

        #Subset to Area, var and run
        cpm.proj <- cpm.proj[grepl(i, cpm.proj)&grepl(v, cpm.proj)&grepl(r, cpm.proj)]

        #Load in 
        proj.df2 <- lapply(cpm.proj, function(x){
          df <- as.data.frame(fread(paste0(fp, x)))
          #Remove x and y cols
          df[c(3:ncol(df))]
          })

          names(proj.df2) <- cpm.proj

        proj.df <- c(list(proj.df1), proj.df2) %>% reduce(cbind)
  
        remove("proj.df1")
        remove("proj.df2")

## **2. Wrangle the data**
    
        #missing.in.hads.cpm.cal <- cal.df[-which(row.names(cal.df)%in%row.names(obs.df)),]
        #missing.in.hads.cpm.proj <- proj.df[-which(row.names(proj.df)%in%row.names(obs.df)),]
  

          cal.df <- cal.df[which(row.names(cal.df)%in%row.names(obs.df)),]
          proj.df <- proj.df[which(row.names(proj.df)%in%row.names(obs.df)),]
 
        #save the missing outputs 
        p <- paste0("checkpoint1", v, "_", i, "_", r, "_")
        print(p)
        #write.csv(missing.in.hads.cpm.cal, paste0(dd, "Debiased/R/QuantileMapping/missing.in.hads/",r,"_",i,"_",v, ".csv"))
  
        ### Update obs data to 360 days

  #The below is a work around with the HADS dataset having 365 days on leap years - this is to be updateed and corrected when the 360 day sampling is better sorted 
      
        #Convert obs to 360 day year - has 40 more vars so remove the ones not in cal
        remove <- c("0229_29", "0430_30", "0731_31", "0930_30", "1130_30")
        remove <- paste0(remove, collapse = "|")
      
        obs.df <- obs.df[,!grepl(remove, names(obs.df))]
        #This still pulls in the 31st Dec 2009 for some reason is in the hads so manual remove
        obs.df <- obs.df[1:ncol(cal.df)]

### Transpose the data sets

      #Obs grid should be cols, observations (time) should be rows for linear scaling

      cal.df <- t(cal.df)
      proj.df <- t(proj.df)
      obs.df <- t(obs.df)


## **3. Empirical Quantile Mapping**

#(from qmap vignette) - fitQmapQUANT estimates values of the empirical cumulative distribution function of observed and
#modelled time series for regularly spaced quantiles. doQmapQUANT uses these estimates to perform
#quantile mapping
      p <- paste0("checkpoint2", v, "_", i, "_", r, "_")
      print(p)

      library(qmap)
      qm1.fit <- fitQmapQUANT(obs.df, cal.df,
                        wet.day = FALSE,
                        qstep = 0.01, 
                        nboot = 1) #nboot number of bootstrap samples used for estimation of the observed quantiles. 


        qm1.hist.a <- doQmapQUANT(cal.df, qm1.fit, type="linear")
        qm1.hist.b <- doQmapQUANT(cal.df, qm1.fit, type="tricub")

        qm1.proj.a <- doQmapQUANT(proj.df, qm1.fit, type="linear")
        qm1.proj.b <- doQmapQUANT(proj.df, qm1.fit, type="tricub")

## **4. Save the data**
        p <- paste0("checkpoint3", v, "_", i, "_", r, "_")
        print(p)
          # Save data - lists of dfs for now (will be easier for assessment)
          results.L <- list(obs.df, cal.df, proj.df, qm1.hist.a, qm1.hist.b, qm1.proj.a, qm1.proj.b)

          names(results.L) <- c("t.obs", "t.cal", "t.proj", "qm1.hist.a", "qm1.hist.b", "qm1.proj.a", "qm1.proj.b")
          p <- paste0("checkpoint4", v, "_", i, "_", r, "_")
          print(p)
          base::saveRDS(results.L, file = paste0(dd, "Debiased/R/QuantileMapping/resultsL", r,"_",i,"_",v, ".RDS"))

          p <- paste0("checkpoint5", v, "_", i, "_", r, "_")
          print(p)
          rm(list=setdiff(ls(), c("v", "i", "r", "var", "Runs")))
          
          gc(reset=TRUE)

           
 } else {
   
#### Precipitation - the HADs variable has is called 'rainfall' 
   dd <- "/mnt/vmfileshare/ClimateData/"
   #Subset to Area
   #HADs grid observational data
   fp <- paste0(dd, "Interim/HadsUK/Data_as_df/")
   files <- list.files(fp)
   obs <- files[grepl(i, files)]
   
   #subset file list to var
   obs.var <-  obs[grepl(v,obs)]
   
   #subset to calibration years 
   obs.varc <- obs.var[grepl("1980", obs.var)]
   obs.df <- fread(paste0(fp, obs.varc))
   obs.df <- as.data.frame(obs.df)
   
   row.names(obs.df) <- paste0(obs.df$x, "_", obs.df$y )
   obs.df$x <- NULL
   obs.df$y <- NULL
   
   #Remove the dates not in the cpm 
   ## find col position of the first cpm date 19801201
   n1 <-min(grep("19801201", names(obs.df)))
   obs.df <- obs.df[c(n1:ncol(obs.df))]
   
   
   #Using 1980 - 2010 as calibration period
   fp <- paste0(dd, "Interim/CPM/Data_as_df/")
   cpm.files <- list.files(fp)
   
   #Calibration years 1980 - 2010  - load in full one for 1980 - 2000
   cpm.cal <- cpm.files[grepl("1980|2000", cpm.files)]
   
   #Subset file list to area 
   cpm.cal <- cpm.cal[grepl(i, cpm.cal)]
   
   #subset to var and run
   cpm.cal.var <- cpm.cal[grepl(v, cpm.cal)&grepl(r, cpm.cal)]
   
   #Load in 
   cal.df <- lapply(cpm.cal.var, function(x){
     df <- fread(paste0(fp, x))
     df <- as.data.frame(df)
     
     row.names(df)<- paste0(df$x, "_", df$y)
     df$x <- NULL
     df$y <- NULL
     return(df)
   })
   
   cal.df <- cal.df %>% reduce(cbind)
   
   #Sub out beyond cal period (2010 - 2020) - ie just keep the calibration here 
   #Keep all of the files with these years - because the naming convention runs 
   #from Nov to following year we need to just take the first 30 days of the one starting with 20091201-
   n2 <- min(grep("20091201-",names(cal.df))) + 29
   
   #This is the first part of the validation dataset, but all the val will be added to the projection df for 
   #the sake of bias correction and assessed separately
   proj.df1 <- cal.df[c((n2+1):ncol(cal.df))]
   cal.df <- cal.df[c(1:n2)]
   
   gc()
   
   yi <- paste0(i,c(2020,2040,2060), collapse="|")
   cpm.proj <- cpm.files[grepl(yi, cpm.files)]
   
   #Subset to Area, var and run
   cpm.proj <- cpm.proj[grepl(i, cpm.proj)&grepl(v, cpm.proj)&grepl(r, cpm.proj)]
   
   #Load in 
   proj.df2 <- lapply(cpm.proj, function(x){
     df <- as.data.frame(fread(paste0(fp, x)))
     #Remove x and y cols
     df[c(3:ncol(df))]
   })
   
   names(proj.df2) <- cpm.proj
   
   proj.df <- c(list(proj.df1), proj.df2) %>% reduce(cbind)
   
   remove("proj.df1")
   remove("proj.df2")
   
   ## **2. Wrangle the data**
   
   #missing.in.hads.cpm.cal <- cal.df[-which(row.names(cal.df)%in%row.names(obs.df)),]
   #missing.in.hads.cpm.proj <- proj.df[-which(row.names(proj.df)%in%row.names(obs.df)),]
   
   
   cal.df <- cal.df[which(row.names(cal.df)%in%row.names(obs.df)),]
   proj.df <- proj.df[which(row.names(proj.df)%in%row.names(obs.df)),]
   
   #save the missing outputs 
   p <- paste0("checkpoint1", v, "_", i, "_", r, "_")
   print(p)
   #write.csv(missing.in.hads.cpm.cal, paste0(dd, "Debiased/R/QuantileMapping/missing.in.hads/",r,"_",i,"_",v, ".csv"))
   
   ### Update obs data to 360 days
   
   #The below is a work around with the HADS dataset having 365 days on leap years - this is to be updateed and corrected when the 360 day sampling is better sorted 
   
   #Convert obs to 360 day year - has 40 more vars so remove the ones not in cal
   remove <- c("0229_29", "0430_30", "0731_31", "0930_30", "1130_30")
   remove <- paste0(remove, collapse = "|")
   
   obs.df <- obs.df[,!grepl(remove, names(obs.df))]
   #This still pulls in the 31st Dec 2009 for some reason is in the hads so manual remove
   obs.df <- obs.df[1:ncol(cal.df)]
   
   ### Transpose the data sets
   
   #Obs grid should be cols, observations (time) should be rows for linear scaling
   
   cal.df <- t(cal.df)
   proj.df <- t(proj.df)
   obs.df <- t(obs.df)
    
    ## **3. Empirical Quantile Mapping**
    
    #(from qmap vignette) - fitQmapQUANT estimates values of the empirical cumulative distribution function of observed and
    #modelled time series for regularly spaced quantiles. doQmapQUANT uses these estimates to perform
    #quantile mapping
    p <- paste0("checkpoint2", v, "_", i, "_", r, "_")
    print(p)
  
  
          qm1.fit <- fitQmapQUANT(obs.df, cal.df,
                          wet.day = TRUE, #If wet.day=TRUE the empirical probability of nonzero observations is found (obs>=0) and the corresponding modelled value is selected as a threshold. All modelled values below this threshold are set to zero. If wet.day is numeric the same procedure is performed after setting all obs to zero. 
                          qstep = 0.01, 
                          nboot = 1) #nboot number of bootstrap samples used for estimation of the observed quantiles. 
  
  
          qm1.hist.a <- doQmapQUANT(cal.df, qm1.fit, type="linear")
          qm1.hist.b <- doQmapQUANT(cal.df, qm1.fit, type="tricub")
          
          qm1.proj.a <- doQmapQUANT(proj.df, qm1.fit, type="linear")
          qm1.proj.b <- doQmapQUANT(proj.df, qm1.fit, type="tricub")
          
          ## **4. Save the data**
          p <- paste0("checkpoint3", v, "_", i, "_", r, "_")
          print(p)
          # Save data - lists of dfs for now (will be easier for assessment)
          results.L <- list(obs.df, cal.df, proj.df, qm1.hist.a, qm1.hist.b, qm1.proj.a, qm1.proj.b)
          
          names(results.L) <- c("t.obs", "t.cal", "t.proj", "qm1.hist.a", "qm1.hist.b", "qm1.proj.a", "qm1.proj.b")
          p <- paste0("checkpoint4", v, "_", i, "_", r, "_")
          print(p)
          base::saveRDS(results.L, file = paste0(dd, "Debiased/R/QuantileMapping/resultsL", r,"_",i,"_",v, ".RDS"))
          
          p <- paste0("checkpoint5", v, "_", i, "_", r, "_")
          print(p)
          rm(list=setdiff(ls(), c("v", "i", "r", "var", "Runs")))
          
          gc(reset=TRUE)

  
 }
      }
    }
}

  ########################## 
  
  #Function for applying the bias correction to a list of dfs (ie rather than reading in the csvs, as above)

apply_qmap_to_cropped_dfL <- function(region, #Region code - needs to relate to the file name in a unique way to subset
                                                var, #Meterological variables
                                                Runs){ #Runs as 05 not Run05

  i <- region   

for(r in Runs){
  for(v in var){
    if(v!="pr"){
      dd <- "/mnt/vmfileshare/ClimateData/"

        obs.df <- cities.Hads.dfs[[i]]
        n <- names(obs.df)
        obs.df <- obs.df[[n[grepl(v, n)&grepl("1980",n)]]] #change to rainfall in this  #1980 is the calibration period - pulls out of the list
        
        row.names(obs.df) <- paste0(obs.df$x, "_", obs.df$y )
        obs.df$x <- NULL
        obs.df$y <- NULL
        
        ci <- min(grep("19801201",names(obs.df)))
        obs.df <- obs.df[,ci:ncol(obs.df)]
        
        #Remove the extra dates -- can be removed in future
        remove <- c(paste0("0229_", v,"_29"), paste0("0430_",v,"_30"), paste0("0731_",v,"_31"), paste0("0930_",v,"_30"), 
                    paste0("1130_",v,"_30"))
        remove <- paste0(remove, collapse = "|")
        
        obs.df <- obs.df[,!grepl(remove, names(obs.df))]
        

        n <- names(cities.cpm.dfs)
        cal.df <- cities.cpm.dfs[[i]]
        n <- names(cal.df)
        ii <- n[grepl(paste0("_",v,"_"),n)&grepl("calibration",n)&grepl(paste0("_",r,"_"), n)]
        cal.df <- cal.df[[ii]] 
        row.names(cal.df) <- paste0(cal.df$x, "_", cal.df$y)
        cal.df$x <- NULL
        cal.df$y <- NULL
        
        #Accidently added in too many dates to the the calibration period -- and need to start the obs period from 1st Dec 1980 --
        #This should be removed for next re-running when the HADs 360 cal and regrouping is run
        ci <- min(grep("19801201",names(obs.df)))
        obs.df <- obs.df[,ci:ncol(obs.df)]
        add.to.proj <-  cal.df[,ncol(obs.df):ncol(cal.df)]
        cal.df <- cal.df[,1:ncol(obs.df)]
        
        n <- names(cities.cpm.dfs)
        proj.df <- cities.cpm.dfs[[i]]
        n <- names(proj.df)
        ii <- n[grepl(paste0("_",v,"_"),n)&grepl("projection",n)&grepl(paste0("_",r,"_"), n)]
        proj.df <- proj.df[[ii]] 
        row.names(proj.df) <- paste0(proj.df$x, "_", proj.df$y)
        proj.df$x <- NULL
        proj.df$y <- NULL
        proj.df <- cbind(add.to.proj, proj.df)
        
        #clean up
        remove(add.to.proj)

## **2. Wrangle the data**

          cal.df <- cal.df[which(row.names(cal.df)%in%row.names(obs.df)),]
          proj.df <- proj.df[which(row.names(proj.df)%in%row.names(obs.df)),]
 
        #This all needs to be updated as currently the dates are not aligning very well at all 
          
### Transpose the data sets

      #Obs grid should be cols, observations (time) should be rows for linear scaling

      cal.df <- t(cal.df)
      proj.df <- t(proj.df)
      obs.df <- t(obs.df)


## **3. Empirical Quantile Mapping**

#(from qmap vignette) - fitQmapQUANT estimates values of the empirical cumulative distribution function of observed and
#modelled time series for regularly spaced quantiles. doQmapQUANT uses these estimates to perform
#quantile mapping
      p <- paste0("checkpoint2", v, "_", i, "_", r, "_")
      print(p)

      library(qmap)
      qm1.fit <- fitQmapQUANT(obs.df, cal.df,
                        wet.day = FALSE,
                        qstep = 0.01, 
                        nboot = 1) #nboot number of bootstrap samples used for estimation of the observed quantiles. 


        qm1.hist.a <- doQmapQUANT(cal.df, qm1.fit, type="linear")
        qm1.hist.b <- doQmapQUANT(cal.df, qm1.fit, type="tricub")

        qm1.proj.a <- doQmapQUANT(proj.df, qm1.fit, type="linear")
        qm1.proj.b <- doQmapQUANT(proj.df, qm1.fit, type="tricub")

## **4. Save the data**
        p <- paste0("checkpoint3", v, "_", i, "_", r, "_")
        print(p)
          # Save data - lists of dfs for now (will be easier for assessment)
          results.L <- list(obs.df, cal.df, proj.df, qm1.hist.a, qm1.hist.b, qm1.proj.a, qm1.proj.b)

          names(results.L) <- c("t.obs", "t.cal", "t.proj", "qm1.hist.a", "qm1.hist.b", "qm1.proj.a", "qm1.proj.b")
          p <- paste0("checkpoint4", v, "_", i, "_", r, "_")
          print(p)
          base::saveRDS(results.L, file = paste0(dd, "Debiased/R/QuantileMapping/three.cities/resultsL", r,"_",i,"_",v, ".RDS"))

          p <- paste0("checkpoint5", v, "_", i, "_", r, "_")
          print(p)
          rm(list=setdiff(ls(), c("v", "i", "r", "var", "Runs")))
          
          gc(reset=TRUE)

           
 } else {
   
#### Precipitation - the HADs variable has is called 'rainfall' 
    
   dd <- "/mnt/vmfileshare/ClimateData/"
   
   obs.df <- cities.Hads.dfs[[i]]
   n <- names(obs.df)
   obs.df <- obs.df[[n[grepl("rainfall", n)&grepl("1980",n)]]] #change to rainfall in this  #1980 is the calibration period - pulls out of the list
   
   row.names(obs.df) <- paste0(obs.df$x, "_", obs.df$y )
   obs.df$x <- NULL
   obs.df$y <- NULL
   
   ci <- min(grep("19801201",names(obs.df)))
   obs.df <- obs.df[,ci:ncol(obs.df)]
   
   #Remove the extra dates -- can be removed in future
   remove <- c("0229_rainfall_29", "0430_rainfall_30", "0731_rainfall_31", "0930_rainfall_30", "1130_rainfall_30")
   remove <- paste0(remove, collapse = "|")
   
   obs.df <- obs.df[,!grepl(remove, names(obs.df))]
   
   
   n <- names(cities.cpm.dfs)
   cal.df <- cities.cpm.dfs[[i]]
   n <- names(cal.df)
   ii <- n[grepl(paste0("_",v,"_"),n)&grepl("calibration",n)&grepl(paste0("_",r,"_"), n)]
   cal.df <- cal.df[[ii]]
   row.names(cal.df) <- paste0(cal.df$x, "_", cal.df$y)
   cal.df$x <- NULL
   cal.df$y <- NULL
   
   #Accidently added in too many dates to the the calibration period -- and need to start the obs period from 1st Dec 1980 --
   #This should be removed for next re-running when the HADs 360 cal and regrouping is run
   ci <- min(grep("19801201",names(obs.df)))
   obs.df <- obs.df[,ci:ncol(obs.df)]
   add.to.proj <-  cal.df[,ncol(obs.df):ncol(cal.df)]
   cal.df <- cal.df[,1:ncol(obs.df)]
   
   n <- names(cities.cpm.dfs)
   proj.df <- cities.cpm.dfs[[i]]
   n <- names(proj.df)
   ii <- n[grepl(paste0("_",v,"_"),n)&grepl("projection",n)&grepl(paste0("_",r,"_"), n)]
   proj.df <- proj.df[[ii]] 
   row.names(proj.df) <- paste0(proj.df$x, "_", proj.df$y)
   proj.df$x <- NULL
   proj.df$y <- NULL
   proj.df <- cbind(add.to.proj, proj.df)
   
   #clean up
   remove(add.to.proj)
   
   ## **2. Wrangle the data**
   
   cal.df <- cal.df[which(row.names(cal.df)%in%row.names(obs.df)),]
   proj.df <- proj.df[which(row.names(proj.df)%in%row.names(obs.df)),]
   
   #This all needs to be updated as currently the dates are not aligning very well at all 
   
   ### Transpose the data sets
   
   #Obs grid should be cols, observations (time) should be rows for linear scaling
   
   cal.df <- t(cal.df)
   proj.df <- t(proj.df)
   obs.df <- t(obs.df)
   
    ## **3. Empirical Quantile Mapping**
    
    #(from qmap vignette) - fitQmapQUANT estimates values of the empirical cumulative distribution function of observed and
    #modelled time series for regularly spaced quantiles. doQmapQUANT uses these estimates to perform
    #quantile mapping
    p <- paste0("checkpoint2", v, "_", i, "_", r, "_")
    print(p)
  
  
          qm1.fit <- fitQmapQUANT(obs.df, cal.df,
                          wet.day = TRUE, #If wet.day=TRUE the empirical probability of nonzero observations is found (obs>=0) and the corresponding modelled value is selected as a threshold. All modelled values below this threshold are set to zero. If wet.day is numeric the same procedure is performed after setting all obs to zero. 
                          qstep = 0.01, 
                          nboot = 1) #nboot number of bootstrap samples used for estimation of the observed quantiles. 
  
  
          qm1.hist.a <- doQmapQUANT(cal.df, qm1.fit, type="linear")
          qm1.hist.b <- doQmapQUANT(cal.df, qm1.fit, type="tricub")
          
          qm1.proj.a <- doQmapQUANT(proj.df, qm1.fit, type="linear")
          qm1.proj.b <- doQmapQUANT(proj.df, qm1.fit, type="tricub")
          
          ## **4. Save the data**
          p <- paste0("checkpoint3", v, "_", i, "_", r, "_")
          print(p)
          # Save data - lists of dfs for now (will be easier for assessment)
          results.L <- list(obs.df, cal.df, proj.df, qm1.hist.a, qm1.hist.b, qm1.proj.a, qm1.proj.b)
          
          names(results.L) <- c("t.obs", "t.cal", "t.proj", "qm1.hist.a", "qm1.hist.b", "qm1.proj.a", "qm1.proj.b")
          p <- paste0("checkpoint4", v, "_", i, "_", r, "_")
          print(p)
          base::saveRDS(results.L, file = paste0(dd, "Debiased/R/QuantileMapping/three.cities/resultsL", r,"_",i,"_",v, ".RDS"))
          
          p <- paste0("checkpoint5", v, "_", i, "_", r, "_")
          print(p)
          rm(list=setdiff(ls(), c("v", "i", "r", "var", "Runs")))
          
          gc(reset=TRUE)

  
 }
      }
}
}