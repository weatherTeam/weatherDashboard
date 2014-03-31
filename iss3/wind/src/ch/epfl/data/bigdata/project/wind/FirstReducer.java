package ch.epfl.data.bigdata.project.wind;

import java.io.IOException;
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
		int maxWind = 0;
		String maxDate = "";
		
		while(inputValue.hasNext())
		{
			String tmp = inputValue.next().toString();
			String[] dateAndWind = tmp.toString().split(",");
			int wind = Integer.parseInt(dateAndWind[1]);
			if (wind > maxWind)
			{
				maxWind = wind;
				maxDate = dateAndWind[0];
			}
			else if(wind == maxWind)
				maxDate += (","+dateAndWind[0]);
		}
		String[] maxDateTab = maxDate.toString().split(",");
		
		for(int i=0; i<maxDateTab.length; i++)
			output.collect(new IntWritable(maxWind), new Text(maxDateTab[i]));
	}
}
