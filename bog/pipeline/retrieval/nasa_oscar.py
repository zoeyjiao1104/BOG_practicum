"""
Written by: Sabina Hartnett
OSCAR ocean current collection class.
"""

import json
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta, timezone
from itertools import product
from pathlib import Path
from utilities import files
# the netCDF4 package requires installation
import netCDF4
import numpy as np

class NasaOscar():
    """
    Initializes a desired datetime, latitude, longitude and radius, finds the
    NASA OSCAR current points most relevant to the parameter values requested, 
    collects and saves those values.
    """

    SOURCE_NAME = 'NASA OSCAR'
    SOURCE_WEBSITE = 'https://podaac.jpl.nasa.gov/dataset/JASON_3_L2_OST_OGDR_GPS'

    def __init__(
            self,
            requested_date=datetime.now(),
            lat=None,
            lon=None,
            radius=None,
            get_values=False,
            store_values=True,
            num_closest_days=7
    ):
        """
        Description of initialized values
        Initializes:
            lat () relevant latitudinal coordinate within [-80, 80]
            lon () relevant longitudinal coordinate within [-180, 180]
            radius (float) radius to use when determining relevant data points around (lat, lon)
            requested_date (datetime object) defaults to the current date
            get_values (bool) default False - whether to collect NASA sea surface current values 
                relevant to the passed parameters
            store_values (bool) default True - when True, a tsv of the relevant values will be 
                stored in the /data subfolder
        """
        self.lat = lat
        self.lon = lon
        self.radius = radius
        self.requested_date = requested_date
        self.data = self.get_measurements(requested_date, num_closest_days)
    

    @classmethod
    def get_current_dataset_dates(cls):
        """
        """
        # Send request and confirm expected HTML received
        url = 'https://podaac-opendap.jpl.nasa.gov/opendap/hyrax/allData/oscar/preview/L4/oscar_third_deg/'
        response = requests.get(url)
        if not response.ok:
            raise Exception("Failed to scrape available "
                "NASA OSCAR dataset dates from the site "
                f"home page. Received a {response.status_code} "
                f"status code with the text:\'{response.text}'.")

        # Parse HTML for dataset references in script tag
        soup = BeautifulSoup(response.text, "html.parser")
        script = soup.find('script')
        dataset_info = json.loads(script.text)
        datasets = dataset_info['dataset']
        
        # Parse dataset names to retrieve days since
        # origin and then convert integers into dates
        dates = []
        origin = datetime(year=1992, month=10, day=5, tzinfo=timezone.utc)
        for entry in datasets:
            days_since_origin = re.search(r'\d+', entry['name']).group()
            date = origin + timedelta(days=int(days_since_origin))
            dates.append(date)

        # Return sorted dates
        dates.sort()
        return dates


    def days_since_origin(self, request_date):
        """
        Get number of days between datetime and Oct 5, 1992 (OSCAR launch)

        Inputs:
            request_date (datetime): date to calculate days from start
        Returns:
            (float) the number of days between request_date and Oct 5, 1992
        """
        days_since_origin = (request_date.date() - date(1992, 10, 5))
        return days_since_origin.days + request_date.hour / 24


    def get_n_closest_days(self, start, max_int=None, n=7):
        """
        Makes list of n closest day numbers without exceeding max_int

        Inputs:
            start (float): (int (calculated using datetime.timedelta.days)) 
                the number of days since October 5, 1992
            max_int (int): max int to include
            n (int): number of days to include (default 7)
        Returns:
            list of dates (as integers) to check
        """
        if max_int == None:
            max_int = int(self.days_since_origin(datetime.now())) + 1
        before = start % 1 < 0.5
        start = int(start)
        smallest, largest = start, start
        ints = [start]
        while len(ints) < n:
            if before:
                smallest -= 1
                ints.append(smallest)
                before = False
            elif largest < max_int:
                largest += 1
                ints.append(largest)
                before = True
            else:
                before = True
        return ints


    def get_measurements(self, requested_date, num_closest_days):
        """
        Collects oscar measurements from nearest collection date

        Inputs:
            requested_date (datetime object) date requested to collect from
                Note: this is ordered hierarchically (first elements are closest
                to the requested date), so returned request file is the most accurate
        returns:
            (pd.DataFrame) with desired data
        """
        days_since_origin = self.days_since_origin(requested_date)
        range_to_check = self.get_n_closest_days(days_since_origin, n=num_closest_days)
        for date_since_origin in range_to_check:
            try:
                return self.make_request(date_since_origin)
            except OSError:
                continue
        raise RuntimeError('Error: no file found within 6 days of {}'.format(requested_date))


    def make_request(self, days_since_origin):
        """
        Attempts to collect data from collection event at days_since_origin

        Inputs:
            days_since_origin (int): date to format into requested file
        Returns:
            (pd.DataFrame) containing data on current speeds and directions
        """
        try:
            return files.load_df(Path('oscar') / (str(days_since_origin) + '.tsv'))
        except FileNotFoundError:
            self.url = 'https://podaac-opendap.jpl.nasa.gov/opendap/allData/oscar/preview/L4/oscar_third_deg/oscar_vel{}.nc.gz'.format(days_since_origin)
            return self.get_measurements_from_netcdf(netCDF4.Dataset(self.url))


    def get_measurements_from_netcdf(self, netcdf):
        """ Get all measurements as pandas dataframe from netcdf """
        longitudes = netcdf['longitude'][:].data
        latitudes = netcdf['latitude'][:].data
        coordinates = np.array(list(product(latitudes, longitudes))).reshape(
            len(latitudes), len(longitudes), 2
        )
        meridonal_currents = np.moveaxis(netcdf['v'][:].data.squeeze(0), 0, -1)
        zonal_currents = np.moveaxis(netcdf['u'][:].data.squeeze(0), 0, -1)

        data = np.concatenate(
            [coordinates, zonal_currents, meridonal_currents],
            axis=-1
        )
        # make 2d array
        data = data.reshape(-1, data.shape[-1])
        df = pd.DataFrame(
            data,
            columns=['latitude', 'longitude', 'zonal_current', 'meriodonal_current']
        )
        # drop null values (land coordinates, for example)
        df = df.dropna()
        # longitudes are given 20 - 420 with overlap. Convert to -180 to 180
        df = df[df['longitude'] < 380]
        df['longitude'] = ((df['longitude'] + 180) % 360) - 180

        # datetime measured in days since 1992-10-05
        base_date = datetime(1992, 10, 5)
        days_since_launch = timedelta(days=int(netcdf['time'][:].data[0]))
        df['datetime'] = base_date + days_since_launch
        df = self.get_degree_and_speed(df)
        files.save_df(df, Path('oscar') / (str(days_since_launch.days) + '.tsv'))
        return df


    def get_degree_and_speed(self, df):
        """
        Calculate current speed and direction from north
            
        Inputs
            df (pd.DataFrame): dataframe with zonal_current and 
                meriodonal_current
            
        Outputs
            df (pd.DataFrame): dataframe with current_direction and
                current_speed columns
        """    
        df['current_direction'] = (
            450 - (
                180 * np.arctan2(df.meriodonal_current, df.zonal_current) / np.pi
            ) 
        ) % 360
        df['current_speed'] = np.sqrt(
            df.meriodonal_current ** 2 + df.zonal_current ** 2
        )
        return df


    def collect_relevant_values(self, store_values):
        """
        Collect and store only the values within the radius of the requested point.
        Ultimately saves a tsv of tuples where each x,y (lat,long) location stores
        a tuple containing the (zonal_curr, meridional_curr) value at that respective lat/long
        (adjusted here to use a longitudinal scale from -180 to 180)
        Where the zonal_curr defines the velocity vector as zonal is east-west 
        and the meridional_curr meridional is north-south
        Initializes:
            lat_array (np array) of latitudes for this data
            lon_array (np array) of longitudes for this data
        Stores:
            tsv of current data within requested radius of requested location
        """
        relevant_data = {}
        self.collect_values()
        self.validate_data() #assert that lat, long and radius have valid values

        for i, la in enumerate(self.lat_array):  # latitude range is [-80, 80]
            if self.lat - self.radius <= la <= min(max(self.lat_array), self.lat + self.radius):
                relevant_data[la] = {} #include only relevant points
                for j, lo in enumerate(self.lon_array):
                    adjusted_lon = lo - 200 #shift from [20, 380] to [-180, 180] range
                    if max(-180, self.lon - self.radius) <= adjusted_lon <= min(180, self.lon + self.radius):
                        relevant_data[la][adjusted_lon] = (self.zonal_curr[0, 0, i, j], self.meridional_curr[0, 0, i, j])
        self.relevant_data = pd.DataFrame.from_dict(relevant_data)

        if store_values:
            collection_days = ''.join(re.findall('\d', self.collection_file))
            files.save_df(self.relevant_data, 'NasaOSCARdata{}'.format(collection_days), output_loc=None, delimiter='\t')


    def print_nc_dataset(self):
        """
        Print the NetCDF4 object.
        from :https://help.ceda.ac.uk/article/4712-reading-netcdf-with-python-opendap
        """
        print('\n[INFO] Global attributes:')
        for attr in dataset.ncattrs():
            print('\t{}: {}'.format(attr, self.NetCDF4_object.getncattr(attr)))

        print('\n[INFO] Variables:\n{}'.format(self.NetCDF4_object.variables))
        print('\n[INFO] Dimensions:\n{}'.format(self.NetCDF4_object.dimensions))


    def get_metadata(self):
        """
        Collects metadata about the NetCDF4 object collected
        from OpenDAP. 
        Initializes:
            collection_type (str) for the OSCAR data: 'Ocean Surface Currents'
            data_type (str) for the OSCAR data: '1/72 YEAR Interval'
            georange (str) for the OSCAR data: '20 to 420 -80 to 80'
            description (str) for the OSCAR data: 'OSCAR Third Degree Sea Surface Velocity'
            period (str) for the OSCAR data: date of collection
        """
        self.collection_type = self.NetCDF4_object.getncattr('VARIABLE')
        self.data_type = self.NetCDF4_object.getncattr('DATATYPE')
        self.georange = self.NetCDF4_object.getncattr('GEORANGE')
        self.description = self.NetCDF4_object.getncattr('DESCRIPTION')
        self.period = self.NetCDF4_object.getncattr('PERIOD')


    def validate_data(self):
        """
        Ensure that all necessary attributes have a value [radius, lat, lon].
        Default radius to collect the full grid [-180, 180] and
        Default latitude and longitude to their respective centermost value.
        """
        if self.radius == None: #include all readings
            self.radius = max(len(self.lon_array), len(self.lat_array))
        assert self.radius >= 0
        
        if self.lat == None:  #default to the centermost latitude
            self.lat = self.lat_array[int(len(self.lat_array)/2)]
        assert min(self.lat_array) <= self.lat <= max(self.lat_array)

        if self.lon == None:  #default to the centermost longitude
            self.lon = self.lon_array[int(len(self.lon_array)/2)]
        assert min(self.lon_array) - 200 <= self.lon <= max(self.lon_array) - 240


    def collect_values(self):
        """
        Assert that time, year, depth, and current array values 
        are valid and collect for the requested object.
        Initializes: (sampled from date = Sept. 5th 2021)
            time: array([days_since_origin], dtype=int32)
            year: array([2021.6805], dtype=float32)
            depth: array([15.], dtype=float32)
            zonal_curr: (array with dim [0][0][481][1201]) holding 
                Ocean Surface Zonal Current data
            meridional_curr: array with dim [0][0][481][1201] holding 
                Ocean Surface Meridional Current data
        """
        assert not self.NetCDF4_object['time'][:].mask
        self.time = self.NetCDF4_object['time'][:].data
        assert not self.NetCDF4_object['year'][:].mask
        self.year = self.NetCDF4_object['year'][:].data
        assert not self.NetCDF4_object['depth'][:].mask
        self.depth = self.NetCDF4_object['depth'][:].data
        assert not self.NetCDF4_object['latitude'][:].mask
        self.latitudes = self.NetCDF4_object['latitude'][:].data
        assert not self.NetCDF4_object['longitude'][:].mask
        # do not include the repeated values in the longitudinal array[:360*3]
        self.longitudes = self.NetCDF4_object['longitude'][:].data[:1080]
        # Ocean Surface Zonal Currents | .shape = (1, 1, 481, 1201) | (time,depth,lat,long)
        self.zonal_curr = self.NetCDF4_object['u'][:].data
        # Ocean Surface Meridional Currents | .shape = (1, 1, 481, 1201) | (time,depth,lat,long)
        self.meridional_curr = self.NetCDF4_object['v'][:].data

if __name__ == "__main__":
    """
    This will run on all the defaults (collecting all data for the most recent
    NASA OSCAR collection) - the user can also instantiate the NasaOscar class
    with the parameters described in the initialization.
    """
    sample_call = NasaOscar(requested_date=pd.to_datetime("2021-11-27T12:03:35"))
    print(sample_call.data)
