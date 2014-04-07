package weather.wind.max;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FirstMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
{
	private static final int MISSING = 9999;
	
	@Override
	public void map(LongWritable inputKey, Text inputValue,
			OutputCollector<IntWritable, Text> output, Reporter arg3) throws IOException
	{
		// wind 65-68
		// geoloc 28-40
		// date 15-26
		// ID 4-9
		
		String quality = inputValue.toString().substring(69, 70);
		String id = inputValue.toString().substring(4, 10);
		String date = inputValue.toString().substring(15, 27);
		String month = (date.substring(4, 6));
		String geoloc = inputValue.toString().substring(28, 40);
		String wind = inputValue.toString().substring(65, 69);

		IntWritable outputKey = new IntWritable(Integer.parseInt(month));
		Text outputValue = new Text(id + date + geoloc + wind);
		
		if(Integer.parseInt(wind) != MISSING && quality.matches("[01459]"))
			output.collect(outputKey, outputValue);
	}
}