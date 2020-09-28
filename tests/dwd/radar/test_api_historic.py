import pytest
from datetime import datetime
from io import BytesIO
from pathlib import Path

from wetterdienst import DWDRadarRequest, TimeResolution
from wetterdienst.dwd.radar.metadata import RadarParameter

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
