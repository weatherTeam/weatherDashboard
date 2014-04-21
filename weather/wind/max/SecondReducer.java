package weather.wind.max;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SecondReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text>
{

	@Override
	public void reduce(IntWritable inputKey, Iterator<Text> inputValue,
			OutputCollector<IntWritable, Text> output, Reporter arg3)
			throws IOException
	{
		Set<Location> closeLocation = new HashSet<Location>();
		
		String[] locationTmp = null;
		
		final ArrayList<String> xtremWindInfo = new ArrayList<String>();
		final ArrayList<String> inputValueArray = new ArrayList<String>();
		
		while (inputValue.hasNext())
		{
			String inputValueString = inputValue.next().toString();
			locationTmp = inputValueString.substring(19, 30).split("[\\+|\\-]");
			
			closeLocation.add(new Location(Integer.parseInt(locationTmp[0]), Integer.parseInt(locationTmp[1])));
			inputValueArray.add(inputValueString);
		}
		
		Location[] closeLocArray = closeLocation.toArray(new Location[closeLocation.size()]);
		
		if (closeLocArray.length > 10)
			for (int i = 0; i < closeLocArray.length; i++)
				for (int j = i; j < closeLocArray.length; j++)
					if ((closeLocArray[i].LAT - closeLocArray[j].LAT) < 1000			
							&& (closeLocArray[i].LON - closeLocArray[j].LON) < 1000 )
						xtremWindInfo.add("+"+closeLocArray[i].LAT+"+"+closeLocArray[i].LON);
		
		for (String value : inputValueArray)
			for (int i = 0; i < xtremWindInfo.size(); i++)
				if (value.contains(xtremWindInfo.get(i)))
				{
					output.collect(inputKey, new Text(value));
					break;
				}
	}
	
	
	public static class Location
	{
		int LAT;
	    int LON;

	    public Location(int first, int second)
	    {
	        this.LAT = first;
	        this.LON = second;
	    }
	}
}
