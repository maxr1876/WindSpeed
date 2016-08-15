import numpy
import csv
import urllib2
import math
import pandas as pd

v_comp = 'http://nomads.ncdc.noaa.gov/thredds/ncss/grid/ndgd/201605/20160525/LVIA98_KWBR_201605251900?var=V-component_of_wind&latitude=42.123587546&longitude=-71.13572160735384&time_start=2013-05-25T18%3A00%3A00Z&time_end=2013-05-25T18%3A00%3A00Z&temporal=point&time=2013-05-25T18%3A00%3A00Z&vertCoord=0&accept=csv&point=true'
u_comp = 'http://nomads.ncdc.noaa.gov/thredds/ncss/grid/ndgd/201605/20160525/LUIA98_KWBR_201605251900?var=U-component_of_wind&latitude=42.123587546&longitude=-71.13572160735384&time_start=2013-05-25T18%3A00%3A00Z&time_end=2013-05-25T18%3A00%3A00Z&temporal=point&time=2013-05-25T18%3A00%3A00Z&vertCoord=0&accept=csv&point=true'
v = urllib2.urlopen(v_comp)
u = urllib2.urlopen(u_comp)
cr = csv.reader(v)

vRead = pd.read_csv(v)
uRead = pd.read_csv(u)
V = vRead['V-component_of_wind[unit="m s-1"]'].iloc[0] #you can also use df['column_name']
U = uRead['U-component_of_wind[unit="m s-1"]'].iloc[0]
# print V, U
DperR = 180.0/math.pi
# print DperR
direction = 270 - (math.atan2(V,U) * DperR)
# speed = math.sqrt(U**2+V**2)
print direction
if direction>0 and direction<=30:
	direction = "N"
elif direction>30 and direction<=60:
	direction = "NE"
elif direction>60 and direction<=120:
	direction = "E"
elif direction>120 and direction<=150:
	direction = "SE"
elif direction>150 and direction<=210:
	direction = "S"
elif direction>210 and direction<=240:
	direction = "SW"
elif direction>240 and direction<=300:
	direction = "W"
elif direction>300 and direction<=330:
	direction = "NW"
elif direction>330:
	direction = "N"

	
print 'Wind direction: ' + str(direction)
print 'Wind speed: ' + str(speed)