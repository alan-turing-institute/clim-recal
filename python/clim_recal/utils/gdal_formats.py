from typing import Final, Literal, get_args

CPM_RAW_PROJ4: Final[
    str
] = "+proj=ob_tran +o_proj=longlat +o_lon_p=0 +o_lat_p=37.5 +lon_0=357.5 +R=6371229 +no_defs=True"

HADS_RAW_PROJ4: Final[
    str
] = "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +a=6377563.396 +rf=299.324961266495 +units=m +no_defs=True"

GDALFormatsType = Literal[
    "VRT",
    "DERIVED",
    "GTiff",
    "COG",
    "NITF",
    "RPFTOC",
    "ECRGTOC",
    "HFA",
    "SAR_CEOS",
    "CEOS",
    "JAXAPALSAR",
    "GFF",
    "ELAS",
    "ESRIC",
    "AIG",
    "AAIGrid",
    "GRASSASCIIGrid",
    "ISG",
    "SDTS",
    "DTED",
    "PNG",
    "JPEG",
    "MEM",
    "JDEM",
    "GIF",
    "BIGGIF",
    "ESAT",
    "FITS",
    "BSB",
    "XPM",
    "BMP",
    "DIMAP",
    "AirSAR",
    "RS2",
    "SAFE",
    "PCIDSK",
    "PCRaster",
    "ILWIS",
    "SGI",
    "SRTMHGT",
    "Leveller",
    "Terragen",
    "netCDF",
    "ISIS3",
    "ISIS2",
    "PDS",
    "PDS4",
    "VICAR",
    "TIL",
    "ERS",
    "JP2OpenJPEG",
    "L1B",
    "FIT",
    "GRIB",
    "RMF",
    "WCS",
    "WMS",
    "MSGN",
    "RST",
    "GSAG",
    "GSBG",
    "GS7BG",
    "COSAR",
    "TSX",
    "COASP",
    "R",
    "MAP",
    "KMLSUPEROVERLAY",
    "WEBP",
    "PDF",
    "Rasterlite",
    "MBTiles",
    "PLMOSAIC",
    "CALS",
    "WMTS",
    "SENTINEL2",
    "MRF",
    "PNM",
    "DOQ1",
    "DOQ2",
    "PAux",
    "MFF",
    "MFF2",
    "GSC",
    "FAST",
    "BT",
    "LAN",
    "CPG",
    "NDF",
    "EIR",
    "DIPEx",
    "LCP",
    "GTX",
    "LOSLAS",
    "NTv2",
    "CTable2",
    "ACE2",
    "SNODAS",
    "KRO",
    "ROI_PAC",
    "RRASTER",
    "BYN",
    "NOAA_B",
    "NSIDCbin",
    "ARG",
    "RIK",
    "USGSDEM",
    "GXF",
    "BAG",
    "S102",
    "HDF5",
    "HDF5Image",
    "NWT_GRD",
    "NWT_GRC",
    "ADRG",
    "SRP",
    "BLX",
    "PostGISRaster",
    "SAGA",
    "XYZ",
    "HF2",
    "OZI",
    "CTG",
    "ZMap",
    "NGSGEOID",
    "IRIS",
    "PRF",
    "EEDAI",
    "DAAS",
    "SIGDEM",
    "EXR",
    "HEIF",
    "TGA",
    "OGCAPI",
    "STACTA",
    "STACIT",
    "JPEGXL",
    "GPKG",
    "OpenFileGDB",
    "CAD",
    "PLSCENES",
    "NGW",
    "GenBin",
    "ENVI",
    "EHdr",
    "ISCE",
    "Zarr",
    "HTTP",
]

GDALGeoTiffFormatStr: Final[str] = get_args(GDALFormatsType)[2]
GDALNetCDFFormatStr: Final[str] = get_args(GDALFormatsType)[42]
GDALVirtualFormatStr: Final[str] = get_args(GDALFormatsType)[0]

TIF_EXTENSION_STR: Final[str] = "tif"
NETCDF_EXTENSION_STR: Final[str] = "nc"
VIRTUAL_EXTENSION_STR: Final[str] = "vrt"

GDALFormatExtensions: Final[dict[str, str]] = {
    GDALGeoTiffFormatStr: TIF_EXTENSION_STR,
    GDALNetCDFFormatStr: NETCDF_EXTENSION_STR,
    GDALVirtualFormatStr: VIRTUAL_EXTENSION_STR,
}
