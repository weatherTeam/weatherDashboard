package weather.wind.max;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SecondReducer extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, Text>
{

	static final ArrayList<Location> closeLocation = new ArrayList<Location>();
	static final ArrayList<ArrayList<String>> infoToOutput = new ArrayList<ArrayList<String>>();
	
	@Override
	public void reduce(IntWritable inputKey, Iterator<Text> inputValue,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException
	{	
		String[] locationTmp = null;
		
		while (inputValue.hasNext())
		{
			String inputValueString = inputValue.next().toString();
			locationTmp = inputValueString.substring(24, 35).split("[\\+|\\-]");

			closeLocation.add(new Location(Integer.parseInt(locationTmp[0]), Integer.parseInt(locationTmp[1]), inputValueString));
		}

		int index = 0;

		while(!closeLocation.isEmpty())
		{
			infoToOutput.add(new ArrayList<String>());
			Location firstElem = closeLocation.remove(0);
			recursiveLocationComputation(firstElem, index);
			index++;
		}
		
		for(int i = 0; i < infoToOutput.size(); ++i)
			for (String s : infoToOutput.get(i))
			{	
				if (i < 10)
					output.collect(new Text(inputKey.toString() +"00"+i), new Text(s));
				else if (i < 100)
					output.collect(new Text(inputKey.toString() +"0"+i), new Text(s));
				else
					output.collect(new Text(inputKey.toString() +i), new Text(s));
			}
		
	}

	public static void recursiveLocationComputation(Location loc, int index)
	{		
		int len = closeLocation.size();
		
		ArrayList<Location> locationToRecall = new ArrayList<Location>();
		
		for (int i = 0; i < len; ++i)
			if (closeTo(loc, closeLocation.get(i)))
				locationToRecall.add(closeLocation.get(i));
	
		if (!locationToRecall.isEmpty())
		{
			for (Location l : locationToRecall)
				closeLocation.remove(l);

			for (Location l : locationToRecall)
				recursiveLocationComputation(l, index);
		}
		
		infoToOutput.get(index).add(loc.infos);
	}
	
	public static boolean closeTo(Location a, Location b)
	{
		return (Math.abs(a.LAT - b.LAT) < 1000 && Math.abs(a.LON - b.LON) < 1000);
	}
	
	public static class Location
	{
		int LAT;
	    int LON;
	    String infos;

	    public Location(int first, int second, String infos)
	    {
	        this.LAT = first;
	        this.LON = second;
	        this.infos = infos;
	    }
	}
}
