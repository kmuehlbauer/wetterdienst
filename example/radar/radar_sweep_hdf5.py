"""
=====
About
=====
Example for DWD radar sites OPERA HDF5 (ODIM_H5) format using wetterdienst and wradlib.

See also:
- https://docs.wradlib.org/en/stable/notebooks/fileio/wradlib_radar_formats.html#OPERA-HDF5-(ODIM_H5)

This program will request the latest RADAR SWEEP_VOL_PRECIPITATION_V data
for Boostedt and plot the outcome with matplotlib.


=====
Setup
=====
::

    brew install gdal
    pip install wradlib

"""
import logging
from tempfile import NamedTemporaryFile

import numpy as np
import wradlib as wrl
import matplotlib.pyplot as pl

from wetterdienst.dwd.radar.metadata import RadarParameter, RadarDate, RadarDataType
from wetterdienst.dwd.radar.sites import RadarSite

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

from wetterdienst import DWDRadarRequest


def plot(data: np.ndarray):
    """
    Convenience function for plotting radar data.
    """

    fig = pl.figure(figsize=(10, 8))
    im = wrl.vis.plot_ppi(data['dataset1/data1/data'], fig=fig, proj='cg')


def radar_info(data: dict):
    """
    Display data from radar request.
    """
    print("Keys:", data.keys())

    log.info("Data")
    for key, value in data.items():
        print(f"- {key}: {value}")


def radar_hdf5_example():

    log.info("Acquiring radar sweep data in HDF5")
    request = DWDRadarRequest(
        radar_parameter=RadarParameter.SWEEP_VOL_PRECIPITATION_V,
        date_times=RadarDate.LATEST.value,
        radar_site=RadarSite.BOO,
        radar_data_type=RadarDataType.HDF5,
    )

    for item in request.collect_data():

        # Decode item.
        timestamp, buffer = item

        # Decode data using wradlib.
        log.info(f"Parsing radar data for {request.radar_site} at '{timestamp}'")

        tempfile = NamedTemporaryFile()
        tempfile.write(buffer.read())
        data = wrl.io.read_opera_hdf5(tempfile.name)

        # FIXME: Make this work.
        #data = wrl.io.read_opera_hdf5(buffer.read())

        # Output debug information.
        radar_info(data)

        # Plot and display data.
        plot(data)
        pl.show()


def main():
    radar_hdf5_example()


if __name__ == "__main__":
    main()
