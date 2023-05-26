rm(list=ls())

library(terra)
library(sp)
library(tidyverse)
library(doParallel)
library(doSNOW)
library(foreach)

Runs <- c("01", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "15")

#these files needs sorting - currently just after RDS
files <- list.files("/Users/rbowyer/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/tempdata/")
files <-files[grepl("df.RDS",files)]
fp <- paste0("/Users/rbowyer/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/tempdata/", files)

setwd("/Users/rbowyer/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/tempdata/")

#Syst time reading df and converting


start <- Sys.time()

for(i in 1:6){

dl <- readRDS(fp[[i]])
df <- dl %>% reduce(cbind)

#df.means_hist <- colMeans(df[c(1:7200)], na.rm=T)
#df.means_hist <- as.data.frame(df.means_hist)
#df.sds_hist <- sapply(df[c(1:7200)], sd, na.rm=T)
#df.sds_hist <- as.data.frame(df.sds_hist)
#df.avs_hist <- cbind(df.means_hist, df.sds_hist) 

#r <- Runs[[i]]
#fn <- paste0("df.avs_hist_Run",i, ".csv")
#write.csv(df.avs_hist, fn)

#df.means_Y21_Y40 <- colMeans(df[c(7201:14400)], na.rm=T)
#df.means_Y21_Y40 <- as.data.frame(df.means_Y21_Y40)
#df.sds_Y21_Y40 <- sapply(df[c(7201:14400)], sd, na.rm=T)
#df.sds_Y21_Y40 <- as.data.frame(df.sds_Y21_Y40)
#df.avs_Y21_Y40 <- cbind(df.means_Y21_Y40, df.sds_Y21_Y40) 

#fn <- paste0("df.Y21_Y40_Run",i, ".csv")
#write.csv(df.avs_Y21_Y40, fn)

df.means_Y61_Y80 <- colMeans(df[c(14401:21600)], na.rm=T)
df.means_Y61_Y80 <- as.data.frame(df.means_Y61_Y80)
df.sds_Y61_Y80 <- sapply(df[c(14401:21600)], sd, na.rm=T)
df.sds_Y61_Y80 <- as.data.frame(df.sds_Y61_Y80)
df.avs_Y61_Y80 <- cbind(df.means_Y61_Y80, df.sds_Y61_Y80) 

fn <- paste0("df.Y61_Y80_Run",i, ".csv")
write.csv(df.avs_Y61_Y80, fn)

remove(dl)
remove(df)
gc()

}
end <- Sys.time()
