from pathlib import Path
from typing import Union

import pandas as pd

from wetterdienst import Parameter, TimeResolution, PeriodType
from wetterdienst.dwd.metadata.constants import (
    DWD_FOLDER_STATION_DATA,
    DWD_FILE_STATION_DATA,
    DataFormat,
)


def store_climate_observations(
    station_data: pd.DataFrame,
    station_id: int,
    parameter: Parameter,
    time_resolution: TimeResolution,
    period_type: PeriodType,
    folder: Union[str, Path],
) -> None:
    """
    Function to store data in a local file hdf file. The function takes a pandas
    DataFrame plus additionally the request parameters to identify data within the
    hdf file and another folder argument for the place where the file is stored.

    :param station_data:            The pandas DataFrame with the obtained data
    :param station_id:              The station id of the station to store
    :param parameter:               Observation measure
    :param time_resolution:         Frequency/granularity of measurement interval
    :param period_type:             Recent or historical files
    :param folder:                  The folder where the hdf is stored

    :return:                        Nothing, only prints information if data was
                                    not stored.
    """
    # Make sure that there is data that can be stored
    if station_data.empty:
        return

    request_string = _build_local_store_key(
        station_id, parameter, time_resolution, period_type
    )

    local_filepath = build_local_filepath_for_station_data(folder)

    local_filepath.parent.mkdir(parents=True, exist_ok=True)

    station_data.to_hdf(path_or_buf=local_filepath, key=request_string)


def restore_climate_observations(
    station_id: int,
    parameter: Parameter,
    time_resolution: TimeResolution,
    period_type: PeriodType,
    folder: Union[str, Path],
) -> pd.DataFrame:
    """
    Function to restore data from a local hdf file based on the place (folder) where
    the file is stored and parameters that define the request in particular.

    :param station_id:              Station id of which data should be restored
    :param parameter:               Observation measure
    :param time_resolution:         Frequency/granularity of measurement interval
    :param period_type:             Recent or historical files
    :param folder:                  The folder where the hdf is stored

    :return:                        All the data.
    """

    request_string = _build_local_store_key(
        station_id, parameter, time_resolution, period_type
    )

    local_filepath = build_local_filepath_for_station_data(folder)

    try:
        # typing required as pandas.read_hdf returns an object by typing
        station_data = pd.read_hdf(path_or_buf=local_filepath, key=request_string)
    except (FileNotFoundError, KeyError):
        return pd.DataFrame()

    # Cast to pandas DataFrame
    station_data = pd.DataFrame(station_data)

    return station_data


def build_local_filepath_for_station_data(folder: Union[str, Path]) -> Union[str, Path]:
    """
    Function to create the local filepath for the station data that is being stored
    in a file if requested.
    Args:
        folder: the given folder where the data should be stored

    Returns:
        a Path build upon the folder
    """
    local_filepath = Path(
        folder,
        DWD_FOLDER_STATION_DATA,
        f"{DWD_FILE_STATION_DATA}.{DataFormat.H5.value}",
    ).absolute()

    return local_filepath


def _build_local_store_key(
    station_id: Union[str, int],
    parameter: Parameter,
    time_resolution: TimeResolution,
    period_type: PeriodType,
) -> str:
    """
    Function that builds a request string from defined parameters including a single
    station id

    :param station_id:              Station id of data
    :param parameter:               Observation measure
    :param time_resolution:         Frequency/granularity of measurement interval
    :param period_type:             Recent or historical files

    :return: A string building a key that is used to identify the request
    """
    request_string = (
        f"{parameter.value}/{time_resolution.value}/"
        f"{period_type.value}/station_id_{int(station_id)}"
    )

    return request_string
