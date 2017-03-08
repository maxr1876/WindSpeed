import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Key = x, y time date
		// Values = rest of row data
		String[] vals = key.toString().split("\t");
		String[] uv = { "", "" };

		// Extract lambert box, time, and date from key (many rows (i.e values)
		// will be mapped to this key)
//		String box = vals[0];
		String time = vals[1];
		String date = vals[2];
		// Reconstruct date with dashes, for use in URL
		String modDate = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6);
		// All rows that are mapped to the key recieved by this reducer will all
		// fall within the same
		// Lambert box, so we only need to query the NOAA server once for any
		// coordinate within the box

		Iterator<Text> iterator = values.iterator();
		String[] lonLatSplit = iterator.next().toString().split(",");
		String lat = lonLatSplit[24], lon = lonLatSplit[25];

		// These are the URLs to retrieve wind data from (note that these
		// may get changed)
		// One particular changed occured from 2016-2017, a switch from http
		// to https)
		
		URL urlV = new URL("https://nomads.ncdc.noaa.gov/thredds/ncss/grid/ndgd/" + date.substring(0, 6) + "/" + date
				+ "/LVIA98_KWBR_" + date + time + "?var=V-component_of_wind&latitude=" + lat + "&longitude=" + lon
				+ "&time_start=" + modDate + "T18%3A00%3A00Z&time_end=" + modDate
				+ "T18%3A00%3A00Z&temporal=point&time=" + modDate + "T18%3A00%3A00Z&vertCoord=0&accept=csv&point=true");
		URL urlU = new URL("https://nomads.ncdc.noaa.gov/thredds/ncss/grid/ndgd/" + date.substring(0, 6) + "/" + date
				+ "/LUIA98_KWBR_" + date + time + "?var=U-component_of_wind&latitude=" + lat + "&longitude=" + lon
				+ "&time_start=" + modDate + "T18%3A00%3A00Z&time_end=" + modDate
				+ "T18%3A00%3A00Z&temporal=point&time=" + modDate + "T18%3A00%3A00Z&vertCoord=0&accept=csv&point=true");
		try {
			uv = getUV(urlU, urlV);
		} catch (Exception e) {
			System.err.println(e);
		}

		// Now that we (hopefully) received the U and V wind components,
		// convert them to speed and direction
		double DperR = 180.0 / Math.PI;
		String dir = "";
		double speed, direction;
		if (uv[0] == null || uv[1] == null) {
			speed = Double.NaN;
			direction = Double.NaN;
		} else {
			direction = (270 - (Math.atan2(Double.parseDouble(uv[1]), Double.parseDouble(uv[0])) * DperR));
			speed = Math.sqrt(Math.pow(Double.parseDouble(uv[1]), 2) + Math.pow(Double.parseDouble(uv[0]), 2));
		}
		if (direction > 0 && direction <= 30)
			dir = "S";
		else if (direction > 30 && direction <= 60)
			dir = "SW";
		else if (direction > 60 && direction <= 120)
			dir = "W";
		else if (direction > 120 && direction <= 150)
			dir = "NW";
		else if (direction > 150 && direction <= 210)
			dir = "N";
		else if (direction > 210 && direction <= 240)
			dir = "NE";
		else if (direction > 240 && direction <= 300)
			dir = "E";
		else if (direction > 300 && direction <= 330)
			dir = "SE";
		else if (direction > 330)
			dir = "S";

		// For each row of data, append the speed and direction, and write
		// it out
		for (Text val : values) {

			context.write(new Text(val + ", " + speed + ", " + dir), new Text(""));
		}

	}

	// Very long method to retrieve U and V components from NOAA server
	// Some days do not have LVIA or LUIA files, so check for LUMA and LVMA
	// instead
	// If LUMA and LVMA also not found, then there is simply no data for the
	// given date
	public String[] getUV(URL urlU, URL urlV) throws IOException, InterruptedException {
		String u = null, v = null;
		URLConnection conV = urlV.openConnection();
		URLConnection conU = urlU.openConnection();
		InputStream in = null;
		try {
			in = conV.getInputStream();
			String encoding = conV.getContentEncoding();
			encoding = encoding == null ? "UTF-8" : encoding;
			String body = IOUtils.toString(in, encoding);
			String[] vDetails = body.split(",");
			if (vDetails.length == 9) {
				v = vDetails[8];
			} else {
				v = "Error getting v-component";
			}
		} catch (IOException ioe) {
			HttpURLConnection httpConn = (HttpURLConnection) conV;
			int statusCode = httpConn.getResponseCode();
			if (statusCode == 404) {
				conV = new URL(urlV.toString().replace("LVIA", "LVMA")).openConnection();
				try {
					in = conV.getInputStream();
					String encoding = conV.getContentEncoding();
					encoding = encoding == null ? "UTF-8" : encoding;
					String body = IOUtils.toString(in, encoding);
					String[] vDetails = body.split(",");
					if (vDetails.length == 9) {
						v = vDetails[8];
					} else {
						v = "Error retrieving v-component";
					}

				} catch (IOException ioe1) {
					String[] ret = { u, v };
					return ret;
				}

			}
		}
		try {
			in = conU.getInputStream();
			String encoding = conU.getContentEncoding();
			encoding = encoding == null ? "UTF-8" : encoding;
			String body = IOUtils.toString(in, encoding);
			String[] uDetails = body.split(",");
			if (uDetails.length == 9) {
				u = uDetails[8];
			} else {
				u = "Error retrieving u-component";
			}
		} catch (IOException ioe) {
			HttpURLConnection httpConn = (HttpURLConnection) conU;
			int statusCode = httpConn.getResponseCode();
			if (statusCode == 404) {
				conV = new URL(urlV.toString().replace("LUIA", "LUMA")).openConnection();
				try {
					in = conU.getInputStream();
					String encoding = conU.getContentEncoding();
					encoding = encoding == null ? "UTF-8" : encoding;
					String body = IOUtils.toString(in, encoding);
					String[] uDetails = body.split(",");
					if (uDetails.length == 9) {
						u = uDetails[8];
					} else {
						u = "Error retrieving u-component";
					}

				} catch (IOException ioe1) {
					String[] ret = { u, v };
					return ret;
				}

			}

		}

		String[] retval = { u, v };
		return retval;
	}

}
