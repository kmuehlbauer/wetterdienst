import re

import pytest
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

from wetterdienst import DWDRadarRequest, TimeResolution
from wetterdienst.dwd.radar.metadata import RadarParameter
from wetterdienst.dwd.radar.sites import RadarSite

HERE = Path(__file__).parent


def test_radar_request_radolan_grid_wrong_parameters():

    with pytest.raises(ValueError):
        DWDRadarRequest(
            radar_parameter=RadarParameter.RADOLAN_GRID,
            time_resolution=TimeResolution.MINUTE_1,
            date_times=["2019-08-08 00:50:00"],
        )


@pytest.mark.remote
def test_radar_request_radolan_grid_hourly():
    """
    Example for testing RADOLAN_GRID.
    """

    request = DWDRadarRequest(
        radar_parameter=RadarParameter.RADOLAN_GRID,
        time_resolution=TimeResolution.HOURLY,
        date_times=["2019-08-08 00:50:00"],
    )

    assert request == DWDRadarRequest(
        radar_parameter=RadarParameter.RADOLAN_GRID,
        time_resolution=TimeResolution.HOURLY,
        date_times=[datetime(year=2019, month=8, day=8, hour=0, minute=50, second=0)],
    )

    with Path(HERE, "radolan_hourly_201908080050").open("rb") as f:
        radolan_hourly = BytesIO(f.read())

    radolan_hourly_test = next(request.collect_data())[1]

    assert radolan_hourly.getvalue() == radolan_hourly_test.getvalue()


@pytest.mark.remote
def test_radar_request_dx_reflectivity_historic():
    """
    Example for testing radar DX site for a specific date.
    """

    tm = datetime.now()

    # One day before.
    tm = tm - timedelta(days=1)

    # Round to the 5 minute mark before tm.
    # https://stackoverflow.com/a/3464000
    tm = tm - timedelta(
        minutes=tm.minute % 5, seconds=tm.second, microseconds=tm.microsecond
    )

    request = DWDRadarRequest(
        radar_parameter=RadarParameter.DX_REFLECTIVITY,
        date_times=[tm],
        radar_site=RadarSite.BOO,
    )

    buffer = next(request.collect_data())[1]
    payload = buffer.getvalue()

    date = bytes(tm.strftime("%d%H%M"), encoding="ascii")
    header = b"DX" + date + b"101320920BY.....VS 2CO0CD4CS0EP0.80.80.80.80.80.80.80.8MS"

    assert re.match(header, payload)
