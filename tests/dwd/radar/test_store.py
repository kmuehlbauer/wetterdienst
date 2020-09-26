from datetime import datetime
from pathlib import PurePath

from wetterdienst import TimeResolution
from wetterdienst.dwd.metadata.constants import DWD_FOLDER_MAIN
from wetterdienst.dwd.radar.metadata import RadarParameter
from wetterdienst.dwd.radar.store import _build_local_filepath


def test_build_local_filepath_for_radar():

    radar_filepath = _build_local_filepath(
        RadarParameter.DX_REFLECTIVITY,
        datetime(2020, 1, 1, 12, 15),
        DWD_FOLDER_MAIN,
        TimeResolution.MINUTE_5,
    )

    assert str(PurePath("dx/5_minutes/dx_5_minutes_202001011215")) in str(radar_filepath)
