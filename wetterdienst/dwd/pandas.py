# Extending pandas
# https://pandas.pydata.org/pandas-docs/stable/development/extending.html
import json

import pandas as pd

from wetterdienst.dwd.metadata import TimeResolution
from wetterdienst.dwd.metadata.column_names import DWDMetaColumns
from wetterdienst.dwd.util import parse_datetime, mktimerange


@pd.api.extensions.register_dataframe_accessor("dwd")
class PandasDwdExtension:
    def __init__(self, pandas_obj):
        self.df = pandas_obj

    def lower(self):
        """
        Make Pandas DataFrame column names and parameters lowercase.

        :return: Mungled DataFrame
        """
        df = self.df.rename(columns=str.lower)

        for attribute in DWDMetaColumns.PARAMETER, DWDMetaColumns.ELEMENT:
            attribute_name = attribute.value.lower()
            if attribute_name in df:
                df[attribute_name] = df[attribute_name].str.lower()

        return df

    def filter_by_date(
        self, date: str, time_resolution: TimeResolution
    ) -> pd.DataFrame:
        """
        Filter Pandas DataFrame by date or date interval.

        Accepts different kinds of date formats, like:

        - 2020-05-01
        - 2020-06-15T12
        - 2020-05
        - 2019
        - 2020-05-01/2020-05-05
        - 2017-01/2019-12
        - 2010/2020

        :param date:
        :param time_resolution:
        :return: Filtered DataFrame
        """

        # Filter by date interval.
        if "/" in date:
            date_from, date_to = date.split("/")
            date_from = parse_datetime(date_from)
            date_to = parse_datetime(date_to)
            if time_resolution in (
                TimeResolution.ANNUAL,
                TimeResolution.MONTHLY,
            ):
                date_from, date_to = mktimerange(time_resolution, date_from, date_to)
                expression = (date_from <= self.df[DWDMetaColumns.FROM_DATE.value]) & (
                    self.df[DWDMetaColumns.TO_DATE.value] <= date_to
                )
            else:
                expression = (date_from <= self.df[DWDMetaColumns.DATE.value]) & (
                    self.df[DWDMetaColumns.DATE.value] <= date_to
                )
            df = self.df[expression]

        # Filter by specific date.
        else:
            date = parse_datetime(date)
            if time_resolution in (
                TimeResolution.ANNUAL,
                TimeResolution.MONTHLY,
            ):
                date_from, date_to = mktimerange(time_resolution, date)
                expression = (date_from <= self.df[DWDMetaColumns.FROM_DATE.value]) & (
                    self.df[DWDMetaColumns.TO_DATE.value] <= date_to
                )
            else:
                expression = date == self.df[DWDMetaColumns.DATE.value]
            df = self.df[expression]

        return df

    def format(self, format: str) -> str:
        """
        Format/render Pandas DataFrame to given output format.

        :param format: One of json, geojson, csv, excel.
        :return: Rendered payload.
        """

        # Output as GeoJSON.
        if format == "geojson":
            output = json.dumps(self.df.dwd.to_geojson(), indent=4)

        else:
            output = self.df.io.format(format=format)

        return output

    def to_geojson(self) -> dict:
        """
        Convert DWD station information into GeoJSON format.

        Args:
            df: Input DataFrame containing station information.

        Return:
             Dictionary in GeoJSON FeatureCollection format.
        """
        df = self.df.rename(columns=str.lower)

        features = []
        for _, station in df.iterrows():
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "id": station["station_id"],
                        "name": station["station_name"],
                        "state": station["state"],
                        "from_date": station["from_date"].isoformat(),
                        "to_date": station["to_date"].isoformat(),
                        "has_file": station["has_file"],
                    },
                    "geometry": {
                        # WGS84 is implied and coordinates represent decimal degrees
                        # ordered as "longitude, latitude [,elevation]" with z expressed
                        # as metres above mean sea level per WGS84.
                        # -- http://wiki.geojson.org/RFC-001
                        "type": "Point",
                        "coordinates": [
                            station["lon"],
                            station["lat"],
                            station["station_height"],
                        ],
                    },
                }
            )

        return {
            "type": "FeatureCollection",
            "features": features,
        }
