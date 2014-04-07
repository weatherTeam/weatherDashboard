package weather.wind.max;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SecondReducer extends MapReduceBase implements Reducer<Text, Text, IntWritable, Text> {
	@Override
	public void reduce(Text inputKey, Iterator<Text> inputValue, OutputCollector<IntWritable, Text> output, Reporter arg3)
			throws IOException
	{	
		int maxWind = 0;
		String maxDate = "";
		
		while(inputValue.hasNext())
		{
			String tmp = inputValue.next().toString();
			String[] dateAndWind = tmp.toString().split("\t");
			int wind = Integer.parseInt(dateAndWind[0]);
			
			if (wind > 28)
			{
				maxWind = wind;
				maxDate = dateAndWind[1];
			}
			else if(wind == maxWind)
				maxDate += (","+dateAndWind[1]);
		}
		output.collect(new IntWritable(maxWind), new Text(maxDate));
	}
}
