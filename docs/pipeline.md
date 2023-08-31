---
title: Analysis pipeline
---
```mermaid

graph TD

data_hads[(HADS)]
data_cpm[(UKCP2.2)]
data_shape_uk[(shapefile UK)]
data_shape_cities[(shapefile cities)]

script_load([ceda_ftp_download.py])

data_hads_raw[RAW/HadsUKgrid/../*.nc]
data_cpm_raw[RAW/UKCP2.2/../*.nc]
data_hads --> script_load
data_cpm --> script_load
script_load --> data_hads_raw
script_load --> data_cpm_raw
data_hads_raw --> script_resampling
data_cpm_raw --> script_reproject

data_shape_uk --> script_crop_uk
data_shape_cities --> script_crop_city

subgraph Preprocessing
    style Preprocessing fill:#f9f9f9,stroke:#333,stroke-width:4px
    
    script_resampling([resampling_hads.py])
    script_reproject([reproject_all.sh])  
    script_preproc([preprocess_data.py])
    script_crop_uk([cropping-CPM-to-Scotland.R])
    script_crop_city([Cropping_rasters_to_three_cities.R])
    
    data_hads_res[Processed/HadsUKgrid/../*.nc]
    data_cpm_rep[Reprojected/UKCP2.2/../*.tiff]
    data_gl[glasgow]
    data_ma[manchester]
    data_lon[london]
    data_out2[out]
    data_out3[out]
    
    script_resampling --> data_hads_res
    script_reproject --> data_cpm_rep
    
    data_hads_res --> script_crop_uk
    data_hads_res --> script_crop_city

    
    script_crop_city --> data_gl
    script_crop_city --> data_ma
    script_crop_city --> data_lon


    data_gl --> script_preproc
    data_ma --> script_preproc
    data_lon --> script_preproc
    script_preproc --> data_out2
    script_crop_uk --> data_out3
    
    data_cpm_rep --> script_crop_city
    data_cpm_rep --> script_crop_uk

end


subgraph Debiasing
    
    script_bc_py([run_cmethods.py])
    script_bc_r([apply_qmapQuant_to_crpd_df_fn.R])
    
    data_out2 --> script_bc_py
    data_gl --> script_bc_r
    data_ma --> script_bc_r
    data_lon --> script_bc_r
    data_out3 --> script_bc_r
    
    
    
end


classDef python fill:#4CAF50;
classDef r fill:#FF5722;
classDef bash fill:#f9f

class script_crop_city,script_crop_uk,script_bc_r r;
class script_load,script_resampling,script_preproc,script_bc_py python;
class script_reproject bash;
```
