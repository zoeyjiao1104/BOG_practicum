# Data Retrieval

The main purpose of this module is to collect oceanographic data from various sources and store it in a standardized format that is easy to use. This format is determined by our database schema so that any additional sources we acquire can easily be added to the database without further changes. All retrieval code is written in classes to help enforce this standardization and avoid repeitious code. 

## Data Format

The core information is stored in a narrow measurements table. This means that rather than having one column for each product we might measure (water temperature, air temperature, wind speed, wind direction, etc.), there is one column to specify which product is being measured, and one column to specify the value of the measurement. Since all products are not measured at each measurement event, this format saves us from many null values. Additionaly, we store the type of measurement (an actual observation? an interpolation? a prediction/forecast? the third quartile from a series of previous measurements?) and quality control information (passed quality checks, failed, suspicious, unknown/untested). Each row in the measurements table references a measurementevents table which records the datetime, latitude, longitude, and sensor information. (If we included this for each measurement, we'd end up duplicating a lot of data if, for example, one buoy measures 10 different products at once).

The measurements and measurementevents table are further broken up into 3 similar tables each, representing three different types of sources from which we get data:
- Stationary data: This data comes from a station that has a fixed location. Because of this, including latitude and longitude in the measurement events table is redundant. We only need a datetime and a reference to a stations table which records the latitude and longitude of the station. NOAA and DFO are examples.
- Mobile data: This data comes from a sensor that can move. Because of this, each measurement event *will* record latitude and longitude (and datetime and a reference to a mobile_sensors data table). BOG buoys are an example.
- Omnipresent data: This is data from something like a satellite, where one sensor takes measurements from a huge range and is located far from where the measurements are actually taken. Because of this, each measurement event *will* record latitude and longitude (and datetime and a reference to a sources table) OSCAR data is an example. 

## Adding new sources

1. Decide whether the source is stationary, mobile, or omnipresent.
2. Subclass MeasurementRetrieval in a new python file.
3. Implement unimplemented methods to extract data from the source
4. Convert that data to our format. The names or measurements should be one of: 'water_level', 'air_temperature', 'water_temperature', 'battery_temperature', 'wind_speed', 'wind_direction', 'current_speed', 'current_direction', 'buoy_speed', 'buoy_direction', or alert the team of a potential new addition. 
5. Find Quality Assurancy / Quality Control flags with data, if any, and convert them to one of: 'good', 'bad', 'suspect', 'na'
6. Find what type of data you are finding, one of: 'observed', 'predicted', 'q1', 'q2', 'q3', 'mean', 'interpolated'

## Source for OISST Dataset
https://www.ncei.noaa.gov/erddap/griddap/ncdc_oisst_v2_avhrr_by_time_zlev_lat_lon.html
Dataset_id also found above. 


For this data set variables can be any set of ["ice", "err", "anom", "sst"]
    where 
        ice = Sea ice concentration (%)
        err = Estimated error standard deviation of analysed_sst (Celsius)
        anom = Daily sea surface temperature anomalies (Celsius)
        sst = Daily sea surface temperature (Celsius)