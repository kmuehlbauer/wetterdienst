from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Union, Optional, List, Generator, Tuple

import pandas as pd

from wetterdienst import TimeResolution, PeriodType
from wetterdienst.dwd.metadata.constants import DWD_FOLDER_MAIN
from wetterdienst.dwd.metadata.column_names import DWDMetaColumns
from wetterdienst.dwd.radar.access import collect_radar_data
from wetterdienst.dwd.radar.index import (
    create_fileindex_radolan_grid,
    create_fileindex_radar,
)
from wetterdienst.dwd.radar.metadata import RadarParameter, RadarDataType, RadarDate
from wetterdienst.dwd.radar.sites import RadarSite
from wetterdienst.dwd.util import parse_enumeration_from_template


class DWDRadarRequest:
    """
    API for DWD RADOLAN data requests.
    """

    def __init__(
        self,
        radar_parameter: Union[str, RadarParameter],
        radar_site: Optional[RadarSite] = None,
        radar_data_type: Optional[RadarDataType] = None,
        date_times: Optional[Union[str, List[Union[str, datetime]]]] = None,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        time_resolution: Optional[Union[str, TimeResolution]] = None,
        period_type: Optional[PeriodType] = None,
        prefer_local: bool = False,
        write_file: bool = False,
        folder: Union[str, Path] = DWD_FOLDER_MAIN,
    ) -> None:
        """

        :param radar_parameter: Radar parameter enumeration for different
                                radar moments and derived data
        :param radar_site:      Site of the radar if parameter is one of
                                RADAR_PARAMETERS_SITES
        :param radar_data_type: Some radar data are available in different data types
        :param date_times:      List of datetimes for which radar data is requested.
                                Minutes have o be defined (HOUR:50), otherwise rounded
                                to 50 minutes as of its provision.
        :param start_date:      Alternative to datetimes, giving a start and end date
        :param end_date:        Alternative to datetimes,
                                giving a start and end date
        :param time_resolution: Time resolution enumeration,
                                either daily or hourly 5 minutes.
                                Only required for radar_parameter == RADOLAN_GRID.
        :param period_type:     period type of PeriodType enumeration
                                Only required for radar_parameter == RADOLAN_GRID.
        :param prefer_local:    Radar data should rather be loaded from disk, for
                                processing purposes
        :param write_file:      File should be stored on drive
        :param folder:          Folder where to store radar data

        :return:                Nothing for now.
        """

        # Convert parameters to enum types.
        self.radar_parameter = RadarParameter(radar_parameter)

        self.radar_site = radar_site
        self.radar_data_type = radar_data_type

        # Sanity checks.
        if (
            self.radar_parameter == RadarParameter.RADOLAN_GRID
            and time_resolution
            not in (
                TimeResolution.HOURLY,
                TimeResolution.DAILY,
                TimeResolution.MINUTE_5,
            )
        ):
            raise ValueError(
                "RADOLAN only supports daily, hourly and 5 minutes resolutions"
            )

        # Convert parameters to enum types.
        self.time_resolution = None
        if time_resolution is not None:
            self.time_resolution = parse_enumeration_from_template(
                time_resolution, TimeResolution
            )

        # Run on of two indexing variants, either for RADOLAN_GRID or generic RADOLAN.
        if self.radar_parameter == RadarParameter.RADOLAN_GRID:
            file_index = create_fileindex_radolan_grid(self.time_resolution)
        else:
            file_index = create_fileindex_radar(
                self.radar_parameter,
                self.time_resolution,
                period_type,
                radar_site,
                radar_data_type,
            )

        self.__build_date_times(file_index, date_times, start_date, end_date)

        if self.radar_parameter == RadarParameter.RADOLAN_GRID:
            self.date_times = self.date_times.dt.floor(freq="H") + pd.Timedelta(
                minutes=50
            )

        self.prefer_local = prefer_local
        self.write_file = write_file
        self.folder = folder

    def __eq__(self, other):
        return (
            self.time_resolution == other.time_resolution
            and self.date_times.values.tolist() == other.date_times.values.tolist()
        )

    def __str__(self):
        return ", ".join(
            [
                self.time_resolution.value,
                "& ".join([str(date_time) for date_time in self.date_times]),
            ]
        )

    def collect_data(self) -> Generator[Tuple[datetime, BytesIO], None, None]:
        """
        Function used to get the data for the request returned as generator.

        :return: For each datetime, the same datetime and file in bytes
        """
        for date_time in self.date_times:
            _, file_in_bytes = collect_radar_data(
                parameter=self.radar_parameter,
                date_times=[date_time],
                time_resolution=self.time_resolution,
                radar_site=self.radar_site,
                radar_data_type=self.radar_data_type,
                write_file=self.write_file,
                folder=self.folder,
            )[0]

            yield date_time, file_in_bytes

    def __build_date_times(
        self,
        file_index: Optional[pd.DataFrame] = None,
        date_times: Optional[Union[str, List[Union[str, datetime]]]] = None,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ):
        """
        builds a pd.Series contains datetime objects
        based on the fileindex or other given parameters
        @todo: use pd.DateTimeIndex instead of pd.Series
        """
        # print("file_index:", file_index["FILENAME"].tolist())
        if date_times == RadarDate.LATEST.value:

            # Try to get latest file by using datetime column.
            # TODO: This is only implemented for RADOLAN_GRID for now.
            try:
                self.date_times = pd.Series(
                    file_index[DWDMetaColumns.DATETIME.value][-1:]
                )

            # TODO: This currently propagates RadarDate.LATEST down the line.
            except KeyError:
                self.date_times = [date_times]
                return

        elif date_times:
            self.date_times = pd.Series(
                pd.to_datetime(date_times, infer_datetime_format=True)
            )

        else:
            self.date_times = pd.Series(
                pd.date_range(
                    pd.to_datetime(start_date, infer_datetime_format=True),
                    pd.to_datetime(end_date, infer_datetime_format=True),
                )
            )

        self.date_times = self.date_times.drop_duplicates().sort_values()
