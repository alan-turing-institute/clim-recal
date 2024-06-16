from dataclasses import dataclass, field
from typing import Collection, Union, Callable, Any, Literal, Final, Iterable
from os import PathLike
from datetime import date
from enum import auto

from .core import StrEnumReprName

AuthorshipType = Union[
    str | tuple[str, ...], dict[str, str] |
    dict[str, dict[str, str]] | dict[str, Collection[str]]
]

class VariableOptions(StrEnumReprName):
    """Supported options for variables"""

    TASMAX = auto()
    RAINFALL = auto()
    TASMIN = auto()

    @classmethod
    def cpm_value(cls, variable: str) -> str:
        """Return `CPM` value equivalent of `variable`.
        
        Parameters
        ----------
        variable
            `VariableOptions` attribute to query value of.

        Examples
        --------
        >>> VariableOptions.cpm_value('rainfall')
        'pr'
        >>> VariableOptions.cpm_value('tasmin')
        'tasmin'
        """
        if variable.lower() == cls.RAINFALL:
            return 'pr'
        else:
            return getattr(cls, variable.upper()).value

    @classmethod
    def cpm_values(cls, variables: Iterable[str] | None = None) -> tuple[str, ...]:
        """Return `CPM` values equivalent of `variable`.
        
        Parameters
        ----------
        variables
            `VariableOptions` attributes to query values of.

        Examples
        --------
        >>> VariableOptions.cpm_values(['rainfall', 'tasmin'])
        ('pr', 'tasmin')
        """
        variables = variables if variables else cls.all()
        return tuple(cls.cpm_value(var) for var in variables)

    @classmethod
    def default(cls) -> str:
        """Default option."""
        return cls.TASMAX.value

    @classmethod
    def all(cls) -> tuple[str, ...]:
        """Return a `tuple` of all options"""
        return tuple(map(lambda c: c.value, cls))


class RunOptions(StrEnumReprName):
    """Supported options for variables.

    Notes
    -----
    Options `TWO` and `THREE` are not available for `UKCP2.2`.
    """

    ONE = "01"
    # TWO = "02"
    # THREE = "03"
    FOUR = "04"
    FIVE = "05"
    SIX = "06"
    SEVEN = "07"
    EIGHT = "08"
    NINE = "09"
    TEN = "10"
    ELEVEN = "11"
    TWELVE = "12"
    THIRTEEN = "13"
    FOURTEEN = "14"
    FIFTEEN = "15"

    @classmethod
    def default(cls) -> str:
        """Default option."""
        return cls.FIVE.value

    @classmethod
    def preferred(cls) -> tuple[str, ...]:
        """Return the prferred runs determined by initial results.

        Notes
        -----
        See `R/misc/Identifying_Runs.md` for motivation and results.
        """
        return (cls.FIVE.value, cls.SIX.value, cls.SEVEN.value, cls.EIGHT.value)

    @classmethod
    def all(cls) -> tuple[str, ...]:
        """Return a `tuple` of all options"""
        return tuple(map(lambda c: c.value, cls))



class MethodOptions(StrEnumReprName):
    """Supported options for methods."""

    QUANTILE_DELTA_MAPPING = auto()
    QUANTILE_MAPPING = auto()
    VARIANCE_SCALING = auto()
    DELTA_METHOD = auto()

    @classmethod
    def default(cls) -> str:
        """Default method option."""
        return cls.QUANTILE_DELTA_MAPPING.value

    @classmethod
    def all(cls) -> tuple[str, ...]:
        """Return a `tuple` of all options"""
        return tuple(map(lambda c: c.value, cls))


class RegionOptions(StrEnumReprName):
    """Supported options for variables."""

    GLASGOW = "Glasgow"
    MANCHESTER = "Manchester"
    LONDON = "London"

    @classmethod
    def default(cls) -> str:
        """Default option."""
        return cls.MANCHESTER.value

    @classmethod
    def all(cls) -> tuple[str, ...]:
        """Return a `tuple` of all options"""
        return tuple(map(lambda c: c.value, cls))


@dataclass
class DataLicense:

    """Class for standardising data license references."""

    name: str
    url: str
    version: int | str | None

    def __str__(self) -> str:
        if self.version:
            return f"{self.name} {self.version}"
        else:
            return self.name


OpenGovernmentLicense = DataLicense(
    name="Open Government License",
    url=("https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"),
    version=1995,
)


@dataclass
class MetaData:

    """Manage info on source material."""

    name: str
    slug: str
    region: str | None = None
    description: str | None= None
    date_created: date | None= None
    authors: AuthorshipType | None = None
    url: str | None = None
    info_url: str | None = None
    doi: str | None = None
    path: PathLike | None = None
    license: str | DataLicense | None = field(default_factory=lambda: OpenGovernmentLicense)
    dates: list[date] | list[int] | None = None
    date_published: date | int | None = None
    unit: str | None = None
    cite_as: str | Callable[[Any], str] | None = None

    def __repr__(self) -> str:
        """Return simple representation of `self` via `slug`."""
        return f'<MetaData(slug="{self.slug}")>'


UKCPLocalProjections = MetaData(
    name="UKCP Local Projections at 2.2 km Resolution for 1980-2080",
    region="UK",
    slug="ukcp",
    url="https://catalogue.ceda.ac.uk/uuid/d5822183143c4011a2bb304ee7c0baf7",
    description="""
Convection permitting climate model projections produced as part of the UK Climate Projection 2018 (UKCP18) project. The data produced by the Met Office Hadley Centre provides information on changes in climate for the UK until 2080, downscaled to a high resolution (2.2 km), helping to inform adaptation to a changing climate.

The projections cover the UK and the time period 1981 to 2080 for a high emissions scenario RCP8.5. Each projection provides an example of climate variability in a changing climate, which is consistent across climate variables at different times and spatial locations.

This dataset contains 2.2 km data for the UK on the 2.2 km rotated pole grid.

Note that the first version of this data covered three time slices (1981-2000, 2021-2040 and 2061-2080), and in March 2023 the remaining time slices (2001-2020, 2041-2060) were added. Also note that the data for the three time slices (1981-2000, 2021-2040 and 2061-2080) were updated during summer 2021, after the correction of a coding error relating to graupel. Full details can be found on the Met Office website, on the UKCP Project News page: https://www.metoffice.gov.uk/research/approach/collaboration/ukcp/ukcp18-project-news/index.
    """,
)

HadUKGrid = MetaData(
    name="HadUK-Grid gridded and regional average climate observations for the UK",
    slug="haduk-grid",
    region="UK",
    url="https://catalogue.ceda.ac.uk/uuid/4dc8450d889a491ebb20e724debe2dfb",
    description="""
    HadUK-Grid is a collection of gridded climate variables derived from the network of UK land surface observations. The data have been interpolated from meteorological station data onto a uniform grid at 1km by 1km to provide complete and consistent coverage across the UK. The 1km data set has been regridded to different resolutions and regional averages to create a collection allowing for comparison to data from UKCP18 climate projections. The dataset spans the period from 1862 to the end of the latest release, but the start time is dependent on climate variable and temporal resolution. The grids are produced for daily, monthly, seasonal and annual timescales, as well as long term averages for a set of climatological reference periods. Variables include air temperature (maximum, minimum and mean), precipitation, sunshine, mean sea level pressure, wind speed, relative humidity, vapour pressure, days of snow lying, and days of ground frost.

This collection supersedes the UKCP09 gridded observations and contains all datasets within the major version 1 release (i.e. v1.#.#.#). As detailed by Hollis et al. (2018, see linked documentation), the version numbering for the dataset follows a pattern x.y.z.θ where:

- X reflects a major upgrade to the whole dataset, for example if a new gridding process was implemented.
- Y reflects a minor upgrade to components of the dataset, for example if a new QC process was adopted.
- Z reflects an addition of the latest data.
- θ is reserved for identifying provisional data, versions of the dataset under development and for identify the source of the data. Prior to June 2023 this was not used within the datasets deposited within the CEDA archive (i.e. upto v1.2.0.0). The use of '.ceda' was instroduced with v1.3.0.ceda to indicate that CEDA was the sources of these data compared to other potential sources during the data production life-cycle.

For example, this collection holds the first release, v1.0.0.0, with data to the end of 2017 and v1.0.1.0 which includes data to the end of 2018 and gridded products at 5km resolution.

The primary purpose of these data are to facilitate monitoring of UK climate and research into climate change, impacts and adaptation. The datasets have been created by the Met Office with financial support from the Department for Business, Energy and Industrial Strategy (BEIS) and Department for Environment, Food and Rural Affairs (DEFRA) in order to support the Public Weather Service Customer Group (PWSCG), the Hadley Centre Climate Programme, and the UK Climate Projections (UKCP18) project. The data recovery activity to supplement 19th and early 20th Century data availability has also been funded by the Natural Environment Research Council (NERC grant ref: NE/L01016X/1) project "Analysis of historic drought and water scarcity in the UK". The collection is provided under Open Government Licence.

Each subsequent version following the initial release is accompanied by change log files. These list new files in the version compared with the previous version plus summary totals of the number of files that remained the same, modified and removed. Links to these change logs are available in the linked documents section of this record.

Additionally for the v1.0.2.1 release a summary change log file has been provided which provides additional details on the changes in processing. Of particular note is the correction to the grid definition for 12 km grid product to match the UKCP18 climate model products in v1.0.2.1 and the inclusion of 5 km resolution gridded data from v1.0.1.0 onwards.
    """,
)

CEDA_ARCHIVE_DATA_SOURCES: Final[dict[str, MetaData]] = {source.slug: source for source in (UKCPLocalProjections, HadUKGrid)}
CEDADataSources = Literal[*CEDA_ARCHIVE_DATA_SOURCES.keys()]
