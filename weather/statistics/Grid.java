package weather.statistics;

/*
 * Copyright (c) Aubry Cholleton
 */

public class Grid {
	
	public static String getGridCoord(String EPSG4326mil, double gridResolutionX, double gridResolutionY) {
		double lat;
		double lon;
		
		if (EPSG4326mil.charAt(0) == '+') {
			lat = Double.parseDouble(EPSG4326mil.substring(1,6));
			lat = lat +  (gridResolutionY-(lat%(gridResolutionY*1000)));
		} else {
			lat = Double.parseDouble(EPSG4326mil.substring(0,6));
			lat = lat - (lat%(gridResolutionY*1000));
		}
		
		if (EPSG4326mil.charAt(6) == '+') {
			lon = Double.parseDouble(EPSG4326mil.substring(7,13));
			lon = lon - (lon%(gridResolutionX*1000));
		} else {
			lon = Double.parseDouble(EPSG4326mil.substring(6,13));
			lon = lon + (gridResolutionX - (lon%(gridResolutionX*1000)));
		}
		
		//lat = lat - (lat%(gridResolutionY*1000));
		//lon = lon - (lon%(gridResolutionX*1000));
		
		
		String x = String.format("%+07d", (int)lon);
		String y= String.format("%+06d", (int)lat); 

		
		return y+","+x;
	}
	
}
