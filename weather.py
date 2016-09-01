
# import requests
# import pandas as pd
# import csv
# resp = requests.get('http://nomads.ncdc.noaa.gov/thredds/ncss/grid/ndgd/201605/20160525/LUIA98_KWBR_201605251900?var=U-component_of_wind&latitude=41.123587546&longitude=-70.13572160735384&time_start=2013-05-25T18%3A00%3A00Z&time_end=2013-05-25T18%3A00%3A00Z&temporal=point&time=2013-05-25T18%3A00%3A00Z&vertCoord=0&accept=csv&point=true')
# v = resp.text
# v = v.split(',')
# print v
# print v[8]

import numpy as np
import csv
import urllib2
import math
import pandas as pd
import csv
import os
import sys
import requests 
import time as t

def isFloat(string):
    try:
        float(string)
        return True
    except (ValueError, AttributeError, TypeError):
        return False

theCSV = sys.argv[1]
r = requests.Session()
if theCSV.endswith(".csv"):
	with open(theCSV, 'rw') as csvfile:
		reader = csv.DictReader(csvfile)
		place = 0
		speeds = []
		dirs = []
		for row in reader:
			date = row['PEAK_NUM'].split('_')[1]
			modDate = date[0:4]+'-'+date[4:6]+'-'+date[6:8]
			lat = row['Lat']
			lon = row['Long']
			rawTime = row['PEAK_NUM'].split('_')[2]
			rawTime = float('.'+str(float(rawTime)/24).split('.')[1])
			time = int(round(rawTime*24.0))
			if time<10:
				time = str('0'+str(time)+'00')
			else:
				time = str(str(time)+'00')

			print time
			v_comp = 'http://nomads.ncdc.noaa.gov/thredds/ncss/grid/ndgd/'+date[0:6]+'/'+date+'/LVIA98_KWBR_'+date+time+'?var=V-component_of_wind&latitude='+lat+'&longitude='+lon+'&time_start='+modDate+'T18%3A00%3A00Z&time_end='+modDate+'T18%3A00%3A00Z&temporal=point&time='+modDate+'T18%3A00%3A00Z&vertCoord=0&accept=csv&point=true'
			u_comp = 'http://nomads.ncdc.noaa.gov/thredds/ncss/grid/ndgd/'+date[0:6]+'/'+date+'/LUIA98_KWBR_'+date+time+'?var=U-component_of_wind&latitude='+lat+'&longitude='+lon+'&time_start='+modDate+'T18%3A00%3A00Z&time_end='+modDate+'T18%3A00%3A00Z&temporal=point&time='+modDate+'T18%3A00%3A00Z&vertCoord=0&accept=csv&point=true'
			# v_comp = 'http://nomads.ncdc.noaa.gov/thredds/ncss/grid/ndgd/201605/20160525/LVIA98_KWBR_201605251900?var=V-component_of_wind&latitude=41.123587546&longitude=-70.13572160735384&time_start=2013-05-25T18%3A00%3A00Z&time_end=2013-05-25T18%3A00%3A00Z&temporal=point&time=2013-05-25T18%3A00%3A00Z&vertCoord=0&accept=csv&point=true'
			# u_comp = 'http://nomads.ncdc.noaa.gov/thredds/ncss/grid/ndgd/201605/20160525/LUIA98_KWBR_201605251900?var=U-component_of_wind&latitude=41.123587546&longitude=-70.13572160735384&time_start=2013-05-25T18%3A00%3A00Z&time_end=2013-05-25T18%3A00%3A00Z&temporal=point&time=2013-05-25T18%3A00%3A00Z&vertCoord=0&accept=csv&point=true'
			v = None
			u = None
			while v is None:
				try:
					v = r.get(v_comp)
					v.raise_for_status()
				except requests.exceptions.HTTPError as e:
					if e.response.status_code == 404:
						try:
							v = r.get(v_comp.replace('LVIA', 'LVMA'))
							v.raise_for_status()
						except requests.exceptions.HTTPError as e:
							if e.response.status_code == 404:
								v = np.nan
					else:
						v = None
					t.sleep(1)
			while u is None:
				try:	
					u = r.get(u_comp)
					u.raise_for_status()
				except requests.exceptions.HTTPError as e:
					if e.response.status_code == 404:
						try:
							u = r.get(u_comp.replace('LUIA', 'LUMA'))
							u.raise_for_status()
						except requests.exceptions.HTTPError as e:
							if e.response.status_code == 404:
								u = np.nan
					else:
						u = None
					t.sleep(1)
			if not isFloat(v):
				# vRead = pd.read_csv(v)
				# V = vRead['V-component_of_wind[unit="m s-1"]'].iloc[0] #you can also use df['column_name']
				try:
					V = float(v.text.split(',')[8])
				except IndexError:
					print v
					sys.exit()
			else:
				V = v
			if not isFloat(u):
				# uRead = pd.read_csv(u)
				# U = uRead['U-component_of_wind[unit="m s-1"]'].iloc[0]
				U = float(v.text.split(',')[8])
			else:
				U = u
		   	if np.isnan(V) or np.isnan(U):
		   		# csv_input = pd.read_csv(theCSV)
				# csv_input['Wind_Speed'].iloc[place] = 'N/A'
				# csv_input['Wind_dir'].iloc[place] = 'N/A'
				speeds.append('N/A')
				dirs.append('N/A')
				place += 1
				print 'Wind direction: ' + str('N/A')
				print 'Wind speed: ' + str('N/A')
				# csv_input.to_csv(theCSV, index=False)
			else:
				DperR = 180.0/math.pi
				direction = 270 - (math.atan2(V,U) * DperR)
				speed = math.sqrt(U**2+V**2)
				if direction>0 and direction<=30:
					direction = "S"
				elif direction>30 and direction<=60:
					direction = "SW"
				elif direction>60 and direction<=120:
					direction = "W"
				elif direction>120 and direction<=150:
					direction = "NW"
				elif direction>150 and direction<=210:
					direction = "N"
				elif direction>210 and direction<=240:
					direction = "NE"
				elif direction>240 and direction<=300:
					direction = "E"
				elif direction>300 and direction<=330:
					direction = "SE"
				elif direction>330:
					direction = "S"

				# csv_input = pd.read_csv(theCSV)
				# csv_input['Wind_Speed'].iloc[place] = speed
				# csv_input['Wind_dir'].iloc[place] = direction
				speeds.append(speed)
				dirs.append(direction)
				print place
				place += 1
				print 'Wind direction: ' + str(direction)
				print 'Wind speed: ' + str(speed)
				# csv_input.to_csv(theCSV, index=False)
		csv_input = pd.read_csv(theCSV)
		csv_input['Wind_Speed'] = speeds
		csv_input['Wind_dir'] = dirs
		csv_input.to_csv(theCSV, index=False)