package weather.temperature.anomalies.grid;

public class Grid {
	
	public static String getGridCoord(String EPSG4326mil, double gridResolutionX, double gridResolutionY) {
		double lat;
		double lon;
		
		if (EPSG4326mil.charAt(0) == '+') {
			lat = Double.parseDouble(EPSG4326mil.substring(1,6));
		} else {
			lat = Double.parseDouble(EPSG4326mil.substring(0,6));
		}
		
		if (EPSG4326mil.charAt(6) == '+') {
			lon = Double.parseDouble(EPSG4326mil.substring(7,13));
		} else {
			lon = Double.parseDouble(EPSG4326mil.substring(6,13));
		}
		
		lat = lat - (lat%(gridResolutionY*1000));
		lon = lon - (lon%(gridResolutionX*1000));
		
		
		String x = String.format("%+07d", (int)lon);
		String y= String.format("%+06d", (int)lat); 
		System.out.println("X:"+x);
		System.out.println("Y:"+y);
		
		return y+x;
	}
	
}
