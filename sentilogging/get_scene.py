import sentinelsat as sentinel
import geopandas as gpd

from typing import OrderedDict
from shapely.geometry import Polygon
from datetime import datetime, timedelta
from ..config import settings


class Sentinel():
    """Main class for download Sentinel-2 data
        Parameters
        ----------
        days_backwards : int
            How many days to watch backwards. default is 30
        id_execution: str
            Execution ID to identify database saving. default
            is None
        dest_dir: str
            Output directory.
    """

    def __init__(self,
                 ref_datetime: datetime,
                 days_backwards: int = 30,
                 id_execution: str = None,
                 dest_dir: str = settings.path.base,
                 api_user: str = settings.sentinel.username,
                 api_password: str = settings.sentinel.password) -> None:

        global logger
        self.ref_datetime = ref_datetime
        self.days_backwards = days_backwards
        self.dest_dir = dest_dir
        self.id_execution = id_execution
        self.count_days_buffer = timedelta(days=days_backwards)
        self.user = api_user
        self.password = api_password
    def api(self) -> sentinel.SentinelAPI:
        """Access SentinelAPI with basic credentials
        from .secrets

        Returns
        -------
        sentinel.SentinelAPI
            Log credentials with SentinelAPI.
        """
        global logger
        api = sentinel.SentinelAPI(self.user,
                                   self.password,
                                   show_progressbars=True)
        api.logger = logger
        return api

    def coords_to_polygon(self,
                          lat_list: list,
                          lon_list: list,
                          crs: str = 'epsg:4326') -> gpd.GeoDataFrame:
        """From lists of latitudes and longitudes, return polygon

        Parameters
        ----------
        lat_list : list
            List of latitudes
        lon_list : list
            List of longitudes
        crs : str
            Specific crs value for polygon definition

        Returns
        -------
        GeoDataFramestr
            GeoDataFrame with resultant polygon and defined crs.
        """
        polygon_geom = Polygon(zip(lon_list, lat_list))
        polygon = gpd.GeoDataFrame(index=[0], crs=crs, geometry=[polygon_geom])

        geojson_poly = polygon.to_file(filename='polygon.geojson', driver='GeoJSON')
        # polygon.to_file(filename='polygon.shp', driver="ESRI Shapefile")

        return geojson_poly

    @staticmethod
    def initial_keywords(self,
                         region: GeoJSON,
                         sensor: str = 'Sentinel-2',
                         data_type: str = None,
                         cloud_coverage: tuple = (0, 75),
                         extra: dict = {}) -> dict:
        """Construct a dict with the keywords to filter the copernicus
        api

        Parameters
        ----------
        region : GeoJSON
            GeoJSON file with polygon.
            generated with self.coords_to_polygon
        sensor : str
            Default sensor is Sentinel-2
        data_type : str, optional
            Possible products to access: S2MSI2A, S2MSI1C, S2MS2Ap
            by default None
        cloud_coverage : tuple, optional
            the cloud coverage accepted in the data, by default (0, 75)
        extra : dict, optional
            include other possible keywords to be used among selection.
            Available keywords at:
            https://scihub.copernicus.eu/userguide/FullTextSearch

        Returns
        -------
        dict
            A dict that contains the keywords utilized to generate the
            query and search the products at copernicus API

        Raises
        ------
        ValueError
            For queries bigger than allowed, this error will raise.
        """
        keywords = {'area': region,
                   'date': (datetime.utcnow() - self.count_days_buffer),
                            datetime.utcnow()),
                   'platformname': sensor,
                   'producttype': data_type,
                   'tileid': None,
                   **extra}

        if sensor == 'Sentinel-2':
            keywords.update(cloudcoverpercentage=cloud_percent_accept)

        query = self.api.format_query(**keywords)

        if self.api.check_query_length(query) > 1.0:
            logger.error("The query string is too long.")
            raise ValueError(f"The query string is too long. Query={query}")

        return keywords

    def search_products(self,
                        cloud_coverage: tuple = (0, 75),
                        data_type: str = None,
                        extra: dict = {}) -> OrderedDict:
        """API search for available products of Sentinel-2
        images with respective cloud coverage.

        Parameters
        ----------
        cloud_coverage : tuple, optional
            Cloud coverage accepted in the data, by default (0, 75)
        data_type : str, optional
            Possible products: S2MSI2A,S2MSI1C, S2MS2Ap
            by default None
        extra : dict, optional
            Possible keywords to be used among selection.
            Available keywords at:
            https://scihub.copernicus.eu/userguide/FullTextSearch

        Returns
        -------
        OrderedDict
            Copernicus API`s response
        """
        region = self.coords_to_polygon(lat_list, lon_list)
        keywords = self.initial_keywords(region, cloud_coverage, extra)
        products = self.api.query(**keywords)

        titles = [i['title'] for i in products.values()]

        logger.info(f'Were found {len(titles)} -> {titles}')

        return products
