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

public class ClusterReducer3 extends MapReduceBase implements
		Reducer<IntWritable, Text, Text, Text>
{
	static final int closeLocationTreshold = 500;
	static final ArrayList<Location> closeLocation = new ArrayList<Location>();
	static ArrayList<String> infoToOutput = new ArrayList<String>();
	
	@Override
	public void reduce(IntWritable inputKey, Iterator<Text> inputValue,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException
	{	
		
		int x = 0, y = 0;
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
			
			closeLocation.add(new Location(x, y, inputValueString));
		}
				
		int index = 0;

		while(!closeLocation.isEmpty())
		{
			Location firstElem = closeLocation.remove(0);
			recursiveLocationComputation(firstElem, index);

			for (String s : infoToOutput)
			{
				if (index < 10)
					output.collect(new Text(inputKey.toString() +"00"+index), new Text(s));
				else if (index < 100)
					output.collect(new Text(inputKey.toString() +"0"+index), new Text(s));
				else
					output.collect(new Text(inputKey.toString() +index), new Text(s));
			}
			infoToOutput = new ArrayList<String>();
			index++;
		}
		
	}

	public static void recursiveLocationComputation(Location loc, int index)
	{	
		int len = closeLocation.size();
		
		if(len > 0)
		{			
			Set<Location> locationToRecall = new HashSet<Location>();
			
			for (int i = 0; i < len; ++i)
				if (closeTo(loc, closeLocation.get(i), closeLocationTreshold))
					locationToRecall.add(closeLocation.get(i));
			
			if (!locationToRecall.isEmpty())
			{
				for (Location l : locationToRecall)
					closeLocation.remove(l);
				
				for (Location l : locationToRecall)
					recursiveLocationComputation(l, index);
			}		
		}
		infoToOutput.add(loc.infos);
	}
	
	public static boolean closeTo(Location a, Location b, int treshold)
	{
		return (Math.abs(a.LAT - b.LAT) < treshold && Math.abs(a.LON - b.LON) < treshold);
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
	    
	    public String toString()
	    {
			return LAT + LON + infos;
		}
	}
}
