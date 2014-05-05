package weather.wind.max;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FindCenterReducer4 extends MapReduceBase implements
		Reducer<Text, Text, Text, Text>
{
	@Override
	public void reduce(Text inputKey, Iterator<Text> inputValue,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException
	{
		int startDate = Integer.MAX_VALUE;
		int endDate = 0;
		int windMax = 0;
//		int rainMax = 0;
		int x = 0;
		int y = 0;
		long totX = 0, totY = 0;
		int nbrPoint = 0;
		int year = 0;
		
		ArrayList<Point> points = new ArrayList<Point>();
			
		while (inputValue.hasNext())
		{
			String inputValueString = inputValue.next().toString();
			
			if (inputValueString.charAt(23) == '+')
				x = Integer.parseInt(inputValueString.substring(24, 29));
			else if (inputValueString.charAt(23) == '-')
				x = Integer.parseInt(inputValueString.substring(23, 29));
			else
				try
				{
					throw new Exception("PROBLEM LONG LAT");
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			if (inputValueString.charAt(29) == '+')
				y = Integer.parseInt(inputValueString.substring(30, 36));
			else if (inputValueString.charAt(29) == '-')
				y = Integer.parseInt(inputValueString.substring(29, 36));
			else
				try
				{
					throw new Exception("PROBLEM LONG LAT");
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			
			points.add(new Point(x, y));
			
			totX += x;
			totY += y;
			
			// find max wind
			int wind = Integer.parseInt(inputValueString.substring(36, 40));
			
			if (windMax < wind)
				windMax = wind;
			
//			// find max rain
//			int rain = Integer.parseInt(inputValueString.substring(42, 46));
//			
//			if (rainMax < rain)
//				rainMax = rain;
			
			// find start date
			int date = Integer.parseInt(inputValueString.substring(15, 23));
			
			if (date < startDate)
				startDate = date;
			if (date > endDate);
				endDate = date;
			year = Integer.parseInt(inputValueString.substring(11, 15));
				
			++nbrPoint;
		}
		
		if (nbrPoint > 1)
		{
			// calculate the center of the event
			int centreX = 0, centreY = 0;
			if (nbrPoint > 0)
			{
				centreX = (int) totX/nbrPoint;
				centreY = (int) totY/nbrPoint;
			}
			
			// find the radius of the event
			Point center = new Point(centreX, centreY);
			double radiusMax = 0.0;
			for (Point p : points)
			{
				double radius = radius(center, p);
				
				if (radiusMax < radius)
					radiusMax = radius;
			}
			
			radiusMax *= 1000;
			
			if (radiusMax > 0)
			{
				String startDateString = startDate + ""; // 12
				String endDateString = endDate + ""; // 12
				String centreXString = centreX + ""; // 6
				String centreYString = centreY + ""; // 6
				String radiusString = (int)radiusMax + ""; // 6
				// windMax // 3
//				String rainMaxString = rainMax + ""; // 3
				
				if (startDate < 10000000)
					startDateString = year + "0" + startDate;
				else
					startDateString = year +""+ startDate;
				if (endDate < 10000000)
					endDateString = year + "0" + endDate;
				else
					endDateString = year +""+ endDate;
				
				if (centreX < 0)
				{
					if (centreX > -10000)
						centreXString = "-00" + Math.abs(centreX);
					else if (centreX > -100000)
						centreXString = "-0" + Math.abs(centreX);	
				}
				else
				{
					if (centreX < 10000)
						centreXString = "+00" + centreX;
					else if (centreX < 100000)
						centreXString = "+0" + centreX;
				}
				
				if (centreY < 0)
				{
					if (centreY > -10000)
						centreYString = "-00" + Math.abs(centreY);
					else if (centreY > -100000)
						centreYString = "-0" + Math.abs(centreY);	
				}
				else
				{
					if (centreY < 10000)
						centreYString = "+00" + centreY;
					else if (centreY < 100000)
						centreYString = "+0" + centreY;
				}	
				
				if (radiusMax < 1)
					radiusString = "000000";
				else if (radiusMax < 10)
					radiusString = "00000" + (int)radiusMax;
				else if (radiusMax < 100)
					radiusString = "0000" + (int)radiusMax;
				else if (radiusMax < 1000)
					radiusString = "000" + (int)radiusMax;
				else if (radiusMax < 10000)
					radiusString = "00" + (int)radiusMax;
				else if (radiusMax < 100000)
					radiusString = "0" + (int)radiusMax;
				
//				if (rainMax < 10)
//					rainMaxString = "00" + rainMax;
//				else if (rainMax < 100)
//					rainMaxString = "0" + rainMax;
				output.collect(new Text(), new Text(startDateString + endDateString + centreXString + centreYString + radiusString + windMax));
//						+ rainMaxString));
			}
		}		
	}
	
	public class Point
	{
		int LAT, LONG;
		
		public Point(int x, int y)
		{
			this.LAT = x;
			this.LONG = y;
		}
		public String toString()
		{
			return "x: "+LAT+"    y: "+LONG;
		}
	}
	
	public double radius(Point x, Point y)
	{
		final int R = 6371; // earth radius
		double phi1 = Math.toRadians((double)x.LAT/1000.0);
		double phi2 = Math.toRadians((double)y.LAT/1000.0);
		double deltaPhi = Math.toRadians((double)y.LAT/1000.0 - (double)x.LAT/1000.0);
		double deltaLambda = Math.toRadians((double)y.LONG/1000.0 - (double)x.LONG/1000.0);
		
		double a = Math.sin(deltaPhi/2.0) * Math.sin(deltaPhi/2.0) +
		        Math.cos(phi1) * Math.cos(phi2) *
		        Math.sin(deltaLambda/2.0) * Math.sin(deltaLambda/2.0);
		
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		
		double d = R * c;
		
		return d;
	}
}
