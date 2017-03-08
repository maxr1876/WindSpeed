import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
public class map1 extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		ExecuteShellCommand shell = new ExecuteShellCommand();
		String text = value.toString();
		String [] lines = text.split("\n");
		String retKey;
		String retVal;
		
		/*This loop grabs each line of input and splits it into columns. Only the four needed columns are
		 * extracted (time, date, lon/lat. Once lat/lon are extracted, call wgrib on them to get their 
		 * corresponding lambert box. Then convert time from HH:MM:SS:MS to nearest HHHH (i.e 1200). Also must convert date
		 * from YYYY-MM-DD to YYYYMMDD*/
		for (String line : lines){
			if (line.contains("date") || line.contains("time")) //Field descriptors can be ignored
				continue;
						
			String [] elements = line.split(","); 
		    String lonLat = elements [25] + " " + elements[24];
		    String time = elements [2];
		    String date = elements [1];
	
		    //Calling wgrib2 to get Lambert box 
		    String result = shell.executeCommand("wgrib2 LVIA98_KWBR_201403241300 -v -lon "+lonLat);

		    //Error checking
		    if (!result.contains("ix") && !result.contains("iy"))
		    	retKey = "Invalid coordinates!";
		    else{
		    	String [] splitResult = result.split(",");
			    String xCoord = splitResult[3].substring(3);
			    String yCoord = splitResult[4].substring(3);
			    // xCoord and yCoord are parsed from the output of wgrib2, set as x, y 
			    retKey = xCoord + ", " + yCoord;
		    }
		       
		    //Now convert date and time (put in try/catch in case there is invalid data for time field)
		    String [] splitTime = time.split(":");
		    try{
		    	int hour = Integer.parseInt(splitTime[0]);
		    	int minute = Integer.parseInt(splitTime[1]);
		    	retVal = convertTime(hour, minute, Double.parseDouble(elements[24]), Double.parseDouble(elements[25]), date);
		    }catch (Exception e){
		    	retVal = "Invalid time!";
		    }
		    
		    //Format date from YYYY-MM-DD to YYYYMMDD
		    date = date.replaceAll("-", "");
		    
		    retVal += "\t" + date.replaceAll("-",  "");
		    //Write out lambert box with box, time (hhhh) as key, entire row of data as value 
		    context.write(new Text(retKey + "\t" + retVal), new Text(line));
		 }
	}
	
	//TODO: Convert time to proper time zone, account for daylight savings
	public String convertTime(int hour, int minute, double lat, double lon, String date) throws ParseException{
		/* Code to get timezone of a coordinate
		TimezoneMapper tz = new TimezoneMapper();
		String timezone = tz.latLngToTimezoneString(40.54417196, -105.1051463);
		System.out.println(timezone);
		*/

		String zone = TimezoneMapper.latLngToTimezoneString(lat, lon);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date date1 = sdf.parse(date);
	    java.util.Date cutoffDate = sdf.parse("2014-09-15");
	    
	    if (date1.compareTo(cutoffDate) >= 0){
	    	switch (zone){
	    	case "America/Indiana/Indianapolis":
	    	case "America/New_York":
	    	case "America/Kentucky/Monticello":
	    	case "America/Kentucky/Louisville":
	    	case "America/Indiana/Vevay":
	    	case "America/Indiana/Winamac":
	    	case "America/Indiana/Vincennes":
	    	case "America/Indiana/Marengo":
	    		hour -= 5; //GMT is 5 hours ahead of Eastern time, so subtract 5 hours here, one hour more
	    		break;	   // per each time zone to the west
	    	case "America/Chicago":
	    	case "America/Montreal":
	    	case "America/Detroit":
	    	case "America/North_Dakota/New_Salem":
	    	case "America/Indiana/Petersburg":
	    	case "America/Indiana/Tell_City":
	    	case "America/Indiana/Knox":
	    	case "America/Menominee":
	    	case "America/North_Dakota/Center":
	    		hour -= 6;
	    		break;
	    	case "America/Denver":
	    	case "America/North_Dakota/Beulah":
	    	case "America/Phoenix":
	    	case "America/Boise":
	    		hour -= 7;
	    		break;
	    	case "America/Los_Angeles":
	    		hour -= 8;
	    		break;
	    	default:
	    		break; 			
	    	}
	    }
	    else{//In this case, date is before the switch, and records are in EST
	    	switch (zone){
	    	case "America/Indiana/Indianapolis":
	    	case "America/New_York":
	    	case "America/Kentucky/Monticello":
	    	case "America/Kentucky/Louisville":
	    	case "America/Indiana/Vevay":
	    	case "America/Indiana/Winamac":
	    	case "America/Indiana/Vincennes":
	    	case "America/Indiana/Marengo":
	    		break; //no change from EST to EST
	    	case "America/Chicago":
	    	case "America/Montreal":
	    	case "America/Detroit":
	    	case "America/North_Dakota/New_Salem":
	    	case "America/Indiana/Petersburg":
	    	case "America/Indiana/Tell_City":
	    	case "America/Indiana/Knox":
	    	case "America/Menominee":
	    	case "America/North_Dakota/Center":
	    		hour -= 1;
	    		break;
	    	case "America/Denver":
	    	case "America/North_Dakota/Beulah":
	    	case "America/Phoenix":
	    	case "America/Boise":
	    		hour -= 2;
	    		break;
	    	case "America/Los_Angeles":
	    		hour -= 3;
	    		break;
	    	default:
	    		break; 			
	    	}
	    }
	    
		if (minute >= 30)
			hour += 1;
		if (hour <10)
			return "0" + Integer.toString(hour) + "00";
		else
			return Integer.toString(hour) + "00";
		
	}
}
