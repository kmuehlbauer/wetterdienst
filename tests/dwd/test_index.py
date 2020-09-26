import pytest

from wetterdienst.dwd.metadata.constants import DWDCDCBase
from wetterdienst.dwd.metadata.parameter import Parameter
from wetterdienst.dwd.metadata.period_type import PeriodType
from wetterdienst import TimeResolution
from wetterdienst.util.network import list_remote_files
from wetterdienst.dwd.index import (
    build_path_to_parameter,
    _create_file_index_for_dwd_server,
)
from wetterdienst.dwd.observations.store import build_local_filepath_for_station_data


def test_build_local_filepath_for_station_data():
    local_filepath = build_local_filepath_for_station_data("dwd_data")

    assert (
        "/".join(local_filepath.as_posix().split("/")[-3:])
        == "dwd_data/station_data/dwd_station_data.h5"
    )


def test_build_index_path():
    path = build_path_to_parameter(
        Parameter.CLIMATE_SUMMARY, TimeResolution.DAILY, PeriodType.HISTORICAL
    )
    assert path == "daily/kl/historical/"


@pytest.mark.remote
def test_list_files_of_climate_observations():
    files_server = list_remote_files(
        "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
        "annual/kl/recent",
        recursive=False,
    )

    assert (
        "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
        "annual/kl/recent/jahreswerte_KL_01048_akt.zip" in files_server
    )


def test_fileindex():

    file_index = _create_file_index_for_dwd_server(
        Parameter.CLIMATE_SUMMARY,
        TimeResolution.DAILY,
        PeriodType.RECENT,
        DWDCDCBase.CLIMATE_OBSERVATIONS,
    )

    assert "daily/kl/recent" in file_index.iloc[0]["FILENAME"]
