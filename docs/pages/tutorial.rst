########
Tutorial
########


=============
Prerequisites
=============
Import modules necessary for general functioning.

.. ipython:: python

    import warnings
    warnings.filterwarnings("ignore")
    import wetterdienst
    from wetterdienst import PeriodType, TimeResolution, Parameter
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    from matplotlib import cm


========
Metadata
========

Which parameters are available?


All available combinations
==========================
.. ipython:: python

    print(wetterdienst.discover_climate_observations())


Daily historical data
=====================
.. ipython:: python

    # Selection of daily historical data
    print(
        wetterdienst.discover_climate_observations(
            time_resolution=TimeResolution.DAILY,
            period_type=PeriodType.HISTORICAL
        )
    )


================
Get station list
================
.. ipython:: python

    metadata_hdp = wetterdienst.metadata_for_climate_observations(
        Parameter.PRECIPITATION_MORE, TimeResolution.DAILY, PeriodType.HISTORICAL)
    print("Number of stations with available data: ", metadata_hdp["HAS_FILE"].sum())
    print("Some of the stations:")
    metadata_hdp.head()
