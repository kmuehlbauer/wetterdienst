import pytest
from datetime import datetime
from io import BytesIO
from pathlib import Path

from wetterdienst.dwd.metadata.time_resolution import TimeResolution
from wetterdienst.dwd.radar.access import _collect_radolan_grid_data

HERE = Path(__file__).parent


@pytest.mark.remote
def test_collect_radolan_grid_hourly():
    with Path(HERE, "radolan_hourly_201908080050").open("rb") as f:
        radolan_hourly = BytesIO(f.read())

    radolan_hourly_test = _collect_radolan_grid_data(
        date_times=[datetime(year=2019, month=8, day=8, hour=0, minute=50)],
        time_resolution=TimeResolution.HOURLY,
    )[0][1]

    assert radolan_hourly.getvalue() == radolan_hourly_test.getvalue()


@pytest.mark.remote
def test_collect_radolan_grid_daily():

    with Path(HERE, "radolan_daily_201908080050").open("rb") as f:
        radolan_daily = BytesIO(f.read())

    radolan_daily_test = _collect_radolan_grid_data(
        date_times=[datetime(year=2019, month=8, day=8, hour=0, minute=50)],
        time_resolution=TimeResolution.DAILY,
    )[0][1]

    assert radolan_daily.getvalue() == radolan_daily_test.getvalue()
