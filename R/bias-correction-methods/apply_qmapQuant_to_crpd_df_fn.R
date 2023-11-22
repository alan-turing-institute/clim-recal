#Re-writing WIP_EQM as a loop, to loop over each segment and hopefully create bias corrected for all UK

##Loading data as created in 'Data_Processing_todf.R'

#Requires
library(tidyverse)
library(data.table)
library(qmap)


apply_qmapQUANT_to_cropped_df <- function(region, #Region code - needs to relate to the file name in a unique way to subset
                                                var, #Meterological variables - as in files
                                                Runs, #Run as in name of files
                                          pd, #Parent directory where dataframes of data are
                                          pd.obs, #Parent directory where dataframes of obs data are
                                          val.startdate, #The first date of the validation period. eg 20101201 All dates before this time will be taken as the calibration
                                    rd, #where to store the results list output buy this
                                          ## These args to be passed to qmapQUANT itself:
                                     wet.day,
                                     qstep, # numeric value between 0 and 1, e.g 0.1. The quantile mapping is fitted only for the quantiles defined by quantile(0,1,probs=seq(0,1,by=qstep).
                                     nboot, #numeric value 1 or greater - nboot number of bootstrap samples used for estimation of the observed quantiles.If nboot==1 the estimation is based on all (and not resampled) data.

                                     type) #interpolation method to use for fitting data to the predictions )(eg linear, tricubic)

                                    {

  i <- region
  pd <- pd
  pd.obs <- pd.obs
  rd <- rd
  qstep <- qstep
  nboot <- nboot
  type <- type

for(r in Runs){
  for(v in var){
       wet.day <- ifelse(v=="pr", T, F)
       obs.files <- list.files(paste0(pd.obs, i), full.names = T)
       obs.files.v <- obs.files[grepl(v, obs.files)]
       obs.df <- fread(obs.files.v)
       obs.df <- as.data.frame(obs.df)

       #subset file list to var
        row.names(obs.df) <- paste0(obs.df$x, "_", obs.df$y)
        obs.df$x <- NULL
        obs.df$y <- NULL

        #Create a data frame for the obs data in the validation period and leave 'obs.df' as the calibration
        n1 <-min(grep(val.startdate, names(obs.df)))
        val.df <- obs.df[c(n1:ncol(obs.df))]
        obs.df <- obs.df[c(1:n1-1)]

        #Load in CPM data - df contains all 100 years
        cpm.files <- list.files(paste0(pd, i), full.names = T)
        cpm.files.v <- cpm.files[grepl(v, cpm.files)&grepl(r, cpm.files)]

        cpm.df <- fread(cpm.files.v)
        cpm.df <- as.data.frame(cpm.df)
        row.names(cpm.df) <- paste0(cpm.df$x, "_", cpm.df$y )
        cpm.df$x <- NULL
        cpm.df$y <- NULL

        #Calibration years 1980 - 2010  - load in full one for 1980 - 2000
        cpm.cal <- cpm.df[c(1:n1-1)]
        cpm.proj <- cpm.df[c(n1:ncol(cpm.df))]
        remove(cpm.df)

        #Some cells missing in hads so subset here jic
        cal.df <- cpm.cal[which(row.names(cpm.cal)%in%row.names(obs.df)),]
        proj.df<- cpm.proj[which(row.names(cpm.proj)%in%row.names(obs.df)),]

        remove(cpm.cal)
        remove(cpm.proj)

      #Grid ref should be cols, observations (time) should be rows

      cal.df <- t(cal.df)
      proj.df <- t(proj.df)
      obs.df <- t(obs.df)


## **3. Empirical Quantile Mapping**

      library(qmap)
      qm1.fit <- fitQmapQUANT(obs.df, cal.df,
                        wet.day = wet.day,
                        qstep = qstep,
                        nboot = nboot) #nboot number of bootstrap samples used for estimation of the observed quantiles.


        qm1.hist <- doQmapQUANT(cal.df, qm1.fit, type=type)
        qm1.proj  <- doQmapQUANT(proj.df, qm1.fit, type=type)

## **4. Save the data**

          # Save data - lists of dfs for now (will be easier for assessment to have all to hand)
          results.L <- list(obs.df, val.df, cal.df, proj.df, qm1.hist, qm1.proj)

          names(results.L) <- c("t.obs", "val.df", "t.cal", "t.proj", "qm1.hist", "qm1.val.proj")
          base::saveRDS(results.L, file = paste0(rd, "/",r,"_",v, ".RDS"))

          rm(list=setdiff(ls(), c("v", "i", "pd", "pd.obs", "rd", "val.startdate", "qstep", "nboot", "type", "r", "var", "Runs")))

          gc(reset=TRUE)
    }
  }
}
