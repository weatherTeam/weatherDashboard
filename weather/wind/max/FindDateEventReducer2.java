package weather.wind.max;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FindDateEventReducer2 extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text>
{

	@Override
	public void reduce(IntWritable inputKey, Iterator<Text> inputValue,
			OutputCollector<IntWritable, Text> output, Reporter arg3)
			throws IOException
	{
		
		final TreeMap<Integer, ArrayList<String>> xtremEvent = new TreeMap<Integer, ArrayList<String>>();
		ArrayList<Integer> sortedKeyList = new ArrayList<Integer>();
		
		while (inputValue.hasNext())
		{
			String value = inputValue.next().toString();
			int key = Integer.parseInt(value.substring(15, 19));

			if (xtremEvent.containsKey(key))
				xtremEvent.get(key).add(value);
			else
			{
				xtremEvent.put(key, new ArrayList<String>());
				xtremEvent.get(key).add(value);
			}
		}
				
		for (Map.Entry<Integer, ArrayList<String>> xtrem : xtremEvent.entrySet())
			sortedKeyList.add(xtrem.getKey());
		
		if (sortedKeyList.size() == 0)
			try
			{
				throw new Exception("ERROR: sortedKeyList is empty!");
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		else if (sortedKeyList.size() == 1)
		{
			ArrayList<String> eventsToOutput = xtremEvent.get(sortedKeyList.get(0));
			int key = sortedKeyList.get(0);
			for (String s : eventsToOutput)
				output.collect(new IntWritable(key), new Text(s));
		}
		else
		{
			ArrayList<ArrayList<String>> xtremEventClustered = new ArrayList<ArrayList<String>>();
			boolean outputNow = false;
			
			for (int i = 0; i < sortedKeyList.size(); ++i)
			{	
				if (xtremEventClustered.size() == 0)
					xtremEventClustered.add(xtremEvent.get(sortedKeyList.get(i)));
				
				if (i + 1 < sortedKeyList.size() && sortedKeyList.get(i+1) - sortedKeyList.get(i) <= 1)
					xtremEventClustered.add(xtremEvent.get(sortedKeyList.get(i+1)));
				else
					outputNow = true;
				
				if(outputNow)
				{
					int outputKey = sortedKeyList.get(i);
					
					for(ArrayList<String> eventsArray : xtremEventClustered)
						for (String s : eventsArray)
							output.collect(new IntWritable(outputKey), new Text(s));							

					xtremEventClustered = new ArrayList<ArrayList<String>>();
					outputNow = false;
				}
			}
		}
		
	}
}
