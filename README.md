# WindSpeed
A short script to read U and V components from NOAA and compute wind speed and direction


To run this script, you must have a csv file with a column containing car_ID, date, and time in format "CFADS2280_20160629_4331.845778
", where the column name is PEAK_NUM. You must also have two columns titled Wind_Speed and Wind_dir to dump results to within the file.
There must also be rows titled Lat and Long that contain latitudes and longitudes of the location you need wind speed/direction for.


Usage:
python weather.py fileName.csv

The output will be the same file as the input, but with data added for wind speed and direction for every row.
