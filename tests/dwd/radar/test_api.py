import re

import pytest
from datetime import datetime
from io import BytesIO
from pathlib import Path

from wetterdienst import DWDRadarRequest, TimeResolution
from wetterdienst.dwd.radar.metadata import RadarParameter, RadarDate, RadarDataType
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
def test_radar_request_rx_reflectivity_latest():
    """
    Example for testing radar COMPOSITES latest.
    """

    request = DWDRadarRequest(
        radar_parameter=RadarParameter.RX_REFLECTIVITY,
        date_times=RadarDate.LATEST.value,
    )

    buffer = next(request.collect_data())[1]
    payload = buffer.getvalue()

    header = (
        b"RX......100000920BY 810134VS 3SW   2.28.1PR E\\+00INT   5GP 900x 900MS "
        b"62<asb,boo,ros,hnr,umd,pro,ess,fld,drs,neu,oft,eis,tur,fbg,mem>"
    )

    assert re.match(header, payload)


@pytest.mark.remote
def test_radar_request_rw_reflectivity_latest():
    """
    Example for testing radar COMPOSITES (RADOLAN) latest.
    """

    request = DWDRadarRequest(
        radar_parameter=RadarParameter.RW_REFLECTIVITY,
        date_times=RadarDate.LATEST.value,
    )

    buffer = next(request.collect_data())[1]
    payload = buffer.getvalue()

    header = (
        b"RW......100000920BY1620145VS 3SW   2.28.1PR E-01INT  60GP 900x 900MF 00000001MS "  # noqa:E501,B950
        b"62<asb,boo,ros,hnr,umd,pro,ess,fld,drs,neu,oft,eis,tur,fbg,mem>"
    )

    assert re.match(header, payload)


@pytest.mark.remote
def test_radar_request_dx_reflectivity_latest():
    """
    Example for testing radar SITES latest.
    """

    request = DWDRadarRequest(
        radar_parameter=RadarParameter.DX_REFLECTIVITY,
        date_times=RadarDate.LATEST.value,
        radar_site=RadarSite.BOO,
    )

    buffer = next(request.collect_data())[1]
    payload = buffer.getvalue()

    header = b"DX......101320920BY.....VS 2CO0CD4CS0EP0.80.80.80.80.80.80.80.8MS"

    assert re.match(header, payload)


@pytest.mark.remote
def test_radar_request_sweep_hdf5_latest():
    """
    Example for testing radar SITES latest,
    this time in OPERA HDF5 (ODIM_H5) format.
    """

    request = DWDRadarRequest(
        radar_parameter=RadarParameter.SWEEP_VOL_PRECIPITATION_V,
        date_times=RadarDate.LATEST.value,
        radar_site=RadarSite.BOO,
        radar_data_type=RadarDataType.HDF5,
    )

    buffer = next(request.collect_data())[1]
    payload = buffer.getvalue()

    assert payload.startswith(b'\x89HDF\r\n')
