#!/usr/bin/env python3
import argparse
import ftplib
import os
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final, Sequence

HADS_FTP_PATH: Final[str] = (
    "/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.2.0.ceda/1km/"
)
CPM_FTP_PATH: Final[str] = "/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/"

DEFAULT_SAVE_PATH: Final[Path] = Path("ceda")
CEDA_ENV_USER_NAME_KEY: Final[str] = "CLIM_RECAL_CEDA_USER_NAME"
CEDA_ENV_PASSWORD_KEY: Final[str] = "CLIM_RECAL_CEDA_PASSWORD"


def check_env_auth() -> bool:
    """Test if CEDA `user_name` and `password` available."""
    user_name: str | None = os.getenv(CEDA_ENV_USER_NAME_KEY)
    password: str | None = os.getenv(CEDA_ENV_PASSWORD_KEY)
    return True if user_name and password else False


def download_ftp(
    input: str, output: str, username: str, password: str, order: int
) -> None:
    """Function to connect to the CEDA archive and download data.

    You need to have a user account and provide your username and
    `FTP` password.

    Parameters
    ----------
    input
        Path where the CEDA data to download is located
        (e.g `/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/1km/tasmin/day/v20220310`
        or top level folder like `/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85` if you want to
        download all files in all sub-directories).
    output
        Path to save the downloaded data - sub-directories will be created automatically under the
        output directory.
    username
        CEDA registered username
    password
        CEDA FPT password (obtained as explained in `https://help.ceda.ac.uk/article/280-ftp`)
    order
        Order in which to run download

        `0`: default order of file from `FTP` server
        `1`: reverse order
        `2`: shuffle.

        This functionality allows to run several downloads in parallel without rewriting files
        that are being downloaded.
    """

    # If directory doesn't exist make it
    Path(output).mkdir(parents=True, exist_ok=True)

    # Change the local directory to where you want to put the data
    os.chdir(output)

    # login to FTP
    f = ftplib.FTP("ftp.ceda.ac.uk", username, password)

    # change the remote directory
    f.cwd(input)

    # list children files:
    filelist = f.nlst()

    if order == 1:
        filelist.reverse()
    elif order == 2:
        random.shuffle(filelist)

    counter = 0
    for file in filelist:
        download = True

        print("Downloading", file)
        current_time = datetime.now().strftime("%H:%M:%S")
        print("Current Time =", current_time)

        # if files already exists in the directory check if is the same size
        # of the one in the server, if is the same do not download file.
        if os.path.isfile(file):
            f.sendcmd("TYPE I")
            size_ftp = f.size(file)
            size_local = os.stat(file).st_size

            if size_ftp == size_local:
                download = False
                print("File exists, will not download")

        if download:
            f.retrbinary("RETR %s" % file, open(file, "wb").write)

        counter += 1
        print(counter, "file downloaded out of", len(filelist))

    print("Finished: ", counter, " files downloaded from ", input)
    # Close FTP connection
    f.close()


@dataclass(kw_only=True)
class HADsCEDADownloadManager:

    user_name: str | None
    password: str | None
    variables: Sequence[str] | None = None
    save_path: os.PathLike = DEFAULT_SAVE_PATH
    reverse: bool = False
    shuffle: bool = False
    change_hierarchy: bool = False
    ftp_path: str = HADS_FTP_PATH
    order: int = 0

    def __post_init__(self) -> None:
        self.user_name = self.user_name or os.getenv(CEDA_ENV_USER_NAME_KEY)
        self.password = self.password or os.getenv(CEDA_ENV_PASSWORD_KEY)
        if self.reverse:
            self.order = 1
        # reverse precedes shuffle
        elif self.shuffle:
            self.order = 2
        if not self.user_name or not self.password:
            raise ValueError(f"Both 'user_name' and 'password' needed.")

    def download(self) -> None:
        if self.change_hierarchy:
            for v in self.variables:
                download_ftp(
                    os.path.join(self.ftp_path, n, v, "day", "latest"),
                    os.path.join(self.save_path, v, n, "latest"),
                    username=self.user_name,
                    password=self.password,
                    order=self.order,
                )
        else:
            download_ftp(
                self.ftp_path,
                str(self.save_path),
                username=self.user_name,
                password=self.password,
                order=self.order,
            )


@dataclass(kw_only=True, repr=False)
class CPMCEDADownloadManager(HADsCEDADownloadManager):
    """Manage downloading raw CPM data."""

    runs: Sequence[str] | None = None
    ftp_path: str = CPM_FTP_PATH

    def download(self) -> None:
        if self.change_hierarchy:
            for n in self.runs:
                for v in self.variables:
                    download_ftp(
                        os.path.join(self.ftp_path, n, v, "day", "latest"),
                        os.path.join(self.save_path, v, n, "latest"),
                        username=self.user_name,
                        password=self.password,
                        order=self.order,
                    )
        else:
            download_ftp(
                self.ftp_path,
                str(self.save_path),
                username=self.user_name,
                password=self.password,
                order=self.order,
            )


if __name__ == "__main__":
    """
    Script to download CEDA data from the command line.

    Note you need to have a user account and provide your username
    and FTP password.

    """
    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument(
        "--input",
        help="Path where the CEDA data to download is located. This can be a path with "
        "or without subdirectories. Set to `/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/`"
        " to download all the raw UKCP2.2 climate projection data used in clim-recal.",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--output",
        help="Path to save the downloaded data",
        required=False,
        default=".",
        type=str,
    )
    parser.add_argument(
        "--username",
        help="Username to connect to the CEDA servers",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--psw",
        help="FTP password to authenticate to the CEDA servers",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--reverse",
        help="Run download in reverse (useful to run downloads in parallel)",
        action="store_true",
    )
    parser.add_argument(
        "--shuffle",
        help="Run download in shuffle mode (useful to run downloads in parallel)",
        action="store_true",
    )
    parser.add_argument(
        "--change_hierarchy",
        help="Change the output sub-directories' hierarchy to fit the Turing "
        "Azure fileshare hierarchy (only applicable to UKCP climate "
        "projection data, i.e. when --input is set to "
        "`/badc/ukcp18/data/land-cpm/uk/2.2km/rcp85/`).",
        action="store_true",
    )

    # Read arguments from command line
    args = parser.parse_args()

    order = 0
    if args.reverse:
        order = 1
    # reverse precedes shuffle
    elif args.shuffle:
        order = 2

    # set a flag if change_hierarchy is set
    change_hierarchy = 0
    if args.change_hierarchy:
        change_hierarchy = 1

    if change_hierarchy == 0:
        download_ftp(args.input, args.output, args.username, args.psw, order)
    elif change_hierarchy == 1:
        # this calls the download_ftp function multiple times to download all the CEDA UKCP data.
        # It reads them in the hierarchy that CEDA uses and converts them to a different hierarchy in
        # the destination fileshare (reverting run number and variable name and removing the "day" level)
        for n in [
            "01",
            "04",
            "05",
            "06",
            "07",
            "08",
            "09",
            "10",
            "11",
            "12",
            "13",
            "15",
        ]:
            for v in [
                "clt",
                "flashrate",
                "hurs",
                "huss",
                "pr",
                "prsn",
                "psl",
                "rls",
                "rss",
                "sfcWind",
                "snw",
                "tas",
                "tasmax",
                "tasmin",
                "uas",
                "vas",
                "wsgmax10m",
            ]:
                download_ftp(
                    os.path.join(args.input, n, v, "day", "latest"),
                    os.path.join(args.output, v, n, "latest"),
                    args.username,
                    args.psw,
                    order,
                )
