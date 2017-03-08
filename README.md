This is a Hadoop MapReduce implementation to add wind speed and wind direction to any dataset that contains spatiotemporal data. The requirements of the dataset are as follows:
1) Data must be stored in csv format
2) Data must contain time, date, latitude and longitude fields
3) Data must be stored in HDFS (or any other distributed file system that will work with Hadoop MapReduce)

This software was tailored to a specific dataset, and use on any other dataset will require direct modification of the source code. For example, given a dataset of the format 'date', 'time', 'O2_level', 'temperature', 'lat', 'lon', the following changes would need to be applied:
  Within map method: 1) The index accesses of date, time, and lat/lon would need to be modified to reflect their position.
                     2) Parsing of time field may need to be modified if date format is anything other than YYYY-MM-DD
                     3) Use of convertTime method will likely need to be removed-it was used in this release because timestamps
                         were recorded in either EST or GMT (changed in 2013), and coordinates falling outside EST had to have                              their time adjusted to query the proper date/time on the NOAA server
  Within reduce method:
                     1) The columns for accessing lat/lon need to be changed to match input data format
                     
 In the future, this software may be updated to allow use for any dataset containing the required fields without requiring modification of the source code.
