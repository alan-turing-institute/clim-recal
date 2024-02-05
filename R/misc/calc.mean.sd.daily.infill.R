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
files <-files[grepl("infill.as.df.RDS",files)]
fp <- paste0("/Users/rbowyer/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/tempdata/", files)

setwd("/Users/rbowyer/Library/CloudStorage/OneDrive-TheAlanTuringInstitute/tempdata/")

#Syst time reading df and converting


for(i in 1:12){

  dl <- readRDS(fp[[i]])
  df <- dl %>% reduce(cbind)

  df.means_hist <- colMeans(df[c(1:7200)], na.rm=T)
  df.means_hist <- as.data.frame(df.means_hist)
  df.sds_hist <- sapply(df[c(1:7200)], sd, na.rm=T)
  df.sds_hist <- as.data.frame(df.sds_hist)
  df.avs_hist <- cbind(df.means_hist, df.sds_hist)

  r <- Runs[[i]]
  fn <- paste0("df.avs_Y00_Y20_Run",i, ".csv")
  write.csv(df.avs_hist, fn)

  df.means_Y41_Y60 <- colMeans(df[c(7201:14400)], na.rm=T)
  df.means_Y41_Y60 <- as.data.frame(df.means_Y41_Y60)
  df.sds_Y41_Y60 <- sapply(df[c(7201:14400)], sd, na.rm=T)
  df.sds_Y41_Y60 <- as.data.frame(df.sds_Y41_Y60)
  df.avs_Y41_Y60 <- cbind(df.means_Y41_Y60, df.sds_Y41_Y60)

  fn <- paste0("df.Y41_Y60_Run",i, ".csv")
  write.csv(df.avs_Y41_Y60, fn)

  remove(dl)
  remove(df)
  gc()

}
