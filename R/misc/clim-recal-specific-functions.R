##Function that extracts first raster in the subfolder specified from vmfileshare
first.rast <- function(x){
  fp <- paste0("/Volumes/vmfileshare/ClimateData/", x, "/01/latest/")
  r <- list.files(fp)[1]
  rp <- paste0(fp,r)
  rast(rp)
}


