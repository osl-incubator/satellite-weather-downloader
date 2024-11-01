# Satellite Weather Downloader

| Xarray | Copernicus |
|:-------------------------:|:-------------------------:|
|<img width="1604" alt="Xarray" src="https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Fxray.readthedocs.io%2Fen%2Fv0.9.0%2F_images%2Fdataset-diagram-logo.png&f=1&nofb=1&ipt=4f24c578ee40cd8ac0634231db6bd24d811fe59658eb2f5f67181f6d720d3f20&ipo=images"> |  <img width="1604" alt="Copernicus" src="https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Fwww.eea.europa.eu%2Fabout-us%2Fwho%2Fcopernicus-1%2Fcopernicus-logo%2Fimage&f=1&nofb=1&ipt=56337423b2d920fcf9b4e9dee584e497a5345fc73b20775730740f0ca215fb38&ipo=images">|

SWD is a system for downloading, transforming and analysing Copernicus weather data using Xarray. The lib is split in two functionalities, `request` and the `@cope` Xarray extension. `request` is responsible for extracting NetCDF4 files from Copernicus API, and the `cope` implements Xarray extensions for transforming and visualizing the files.

## Installation
The app is available on PYPI, you can use the package without deploying the containers with the command in your shell:
``` bash
$ pip install satellite-weather-downloader
```

## Requirements
For downloading data from [Copernicus API](https://cds.climate.copernicus.eu/#!/home), it is required an account. The credentials for your account can be found in Copernicus' User Page, in the `API key` section. User API Key will be needed in order to request data, pass them to the in request's `api_key` parameter.


## Create requests via Interactive shell
```python
from satellite import request

dataset = request.reanalysis_era5_land(
    output='my_dataset_file'
    # Any ERA5 Land Reanalysis option can be passed in the method
)
```
```
NOTE: see notebooks/ to more examples
```

## Extract Brazil NetCDF4 file from a date range
``` python
dataset = request.reanalysis_era5_land(
  "bra_dataset"
  locale='BRA',
  date='2023-01-01/2023-01-07'
)

```

## Load the dataset
``` python
from satellite import DataSet
dataset = DataSet.from_netcdf("bra_dataset.zip")

```

## Usage of `cope` extension
``` python
from satellite import ADM2
rio_adm = ADM2.get(code=3304557, adm0="BRA") # Rio de Janeiro's geocode (IBGE)
dataset.cope.to_dataframe(rio_adm)
```

It is also possible to create a dataframe directly from the National-wide dataset:
``` python
rio_ds = dataset.cope.adm_ds(rio_adm)
```

All Xarray methods are extended when using the `copebr` extension:
``` python
rio_ds.precip_tot.to_array()
rio_ds.temp_med.plot()
