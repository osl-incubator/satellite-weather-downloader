from typing import Union

from loguru import logger
from shapely.geometry.polygon import Polygon # type: ignore
from matplotlib.path import Path # type: ignore
import xarray as xr

from . import brazil # type: ignore

@xr.register_dataset_accessor('DSEI')
class CopeDSEIDatasetExtension:
    """
    xarray.Dataset.DSEI
    -------------------

    Usage:
        ```
        import satellite_weather as sat
        ds = sat.load_dataset('file/path')
        ds.DSEI['Yanomami']
        ds.DSEI.get_polygon('Yanomami')
        ```
    """

    DSEIs = brazil.DSEI.areas.DSEI_DF
    _dsei_df = None

    def __init__(self, xarray_ds: xr.Dataset) -> None:
        self._ds = xarray_ds
        self._grid = self.__do_grid()

    def load_polygons(self):
        df = brazil.DSEI.areas.load_polygons_df()
        self._dsei_df = df
        logger.info('DSEI Polygons loaded')

    def get_polygon(self, dsei: Union[str, int]) -> Polygon:
        if self._dsei_df is None:
            logger.error(
                'Polygons are not loaded. Use `.DSEI.load_poligons()`'
            )
            return None

        polygon = self.__do_polygon(dsei)
        return polygon

    def __getitem__(self, __dsei: Union[str, int] = None):
        try:
            return self.__do_dataset(__dsei)
        except AttributeError:
            if self._dsei_df is None:
                logger.error(
                    'Polygons are not loaded. Use `.DSEI.load_poligons()`'
                )
                return None
            logger.error(
                f'{__dsei} not found. List all DSEIs with `.DSEI.info()`'
            )
            return None

    def __do_grid(self):
        x, y = np.meshgrid(self._ds.longitude, self._ds.latitude)
        x, y = x.flatten(), y.flatten()
        grid = np.vstack((x, y)).T
        return grid

    def __do_polygon(self, __dsei: Union[str, int]) -> Polygon:
        if isinstance(__dsei, str):
            cod = float(self.DSEIs[self.DSEIs.DSEI == __dsei].code)
            polygon = self._dsei_df[
                self._dsei_df.cod_dsei == cod
            ].geometry.item()
        elif isinstance(__dsei, int):
            polygon = self._dsei_df[
                self._dsei_df.cod_dsei == float(__dsei)
            ].geometry.item()
        return polygon

    def __do_dataset(self, __dsei: Union[str, int]) -> xr.Dataset:
        polygon = self.__do_polygon(__dsei)
        path_coords = Path(list(polygon.exterior.coords))
        p_filter = path_coords.contains_points(self._grid)

        lons, lats = self._grid[p_filter].T
        ds = self._ds.sel(latitude=lats, longitude=lons)
        return ds
