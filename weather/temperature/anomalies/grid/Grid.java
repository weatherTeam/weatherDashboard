package weather.temperature.anomalies.grid;

public class Grid {
	
	public static String getGridCoord(String EPSG4326mil, int gridResolutionX, int gridResolutionY) {
		Integer lat;
		Integer lon;
		
		if (EPSG4326mil.charAt(0) == '+') {
			lat = Integer.parseInt(EPSG4326mil.substring(1,6));
		} else {
			lat = Integer.parseInt(EPSG4326mil.substring(0,6));
		}
		
		if (EPSG4326mil.charAt(6) == '+') {
			lon = Integer.parseInt(EPSG4326mil.substring(7,13));
		} else {
			lon = Integer.parseInt(EPSG4326mil.substring(6,13));
		}
		
		lat = lat - (lat%(gridResolutionY*1000));
		lon = lon - (lon%(gridResolutionX*1000));
		
		
		String x = String.format("%+07d", lon);
		String y= String.format("%+06d", lat); 
		System.out.println("X:"+x);
		System.out.println("Y:"+y);
		
		return y+x;
	}
	
}
