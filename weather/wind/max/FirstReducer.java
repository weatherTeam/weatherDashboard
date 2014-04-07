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

public class FirstReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
	@Override
	public void reduce(IntWritable inputKey, Iterator<Text> inputValue, OutputCollector<IntWritable, Text> output, Reporter arg3)
			throws IOException
	{	
		ArrayList<String> xtremWindAndDate = new ArrayList<String>();
		
		while(inputValue.hasNext())
		{
			String tmp = inputValue.next().toString();
			String[] dateAndWind = tmp.toString().split(",");
			int wind = Integer.parseInt(dateAndWind[1]);
			
			if (wind > 28)
				xtremWindAndDate.add(dateAndWind[0]+","+dateAndWind[1]);
		}
		
		String[] dateAndWind = new String[xtremWindAndDate.size()];
		
		for(int i=0; i<xtremWindAndDate.size(); i++)
		{
			dateAndWind = xtremWindAndDate.get(i).split(",");
			output.collect(new IntWritable(Integer.parseInt(dateAndWind[1])), new Text(dateAndWind[0]));
		}
	}
}
