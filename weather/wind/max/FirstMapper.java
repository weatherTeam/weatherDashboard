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
		// ID 4-10
		// date 15-27
		// geoloc 28-40
		// wind 65-69
		// rain x, y
		/*
		 * 6 = shower
		 * 7 = thunderstorm
		 * 02 = rain
		 * 05, 06, 07
		 */
		
		String windQuality = inputValue.toString().substring(69, 70);
		String id = inputValue.toString().substring(4, 10);
		String date = inputValue.toString().substring(15, 27);
		String month = (date.substring(4, 6));
		String geoloc = inputValue.toString().substring(28, 40);
		String wind = inputValue.toString().substring(65, 69);
		// String rain = inputValue.toString().substring(x, y);
		// String rainQuality = inputValue.toString().substring(a, b);

		IntWritable outputKey = new IntWritable(Integer.parseInt(month));
		Text outputValue = new Text(id + date + geoloc + wind);
		// Text outputValue = new Text(id + date + geoloc + wind + rain);

		if(Integer.parseInt(wind) != MISSING && windQuality.matches("[01459]") && Integer.parseInt(wind) > 0)
		// if(Integer.parseInt(wind) != MISSING && windQuality.matches("[01459]")
		// && Integer.parseInt(rain) != MISSING && rainQuality.matches("[?????]")
			output.collect(outputKey, outputValue);
		
		// the output key is by monthes
		// the output value contains all informations (id + date + geoloc + wind)
	}
}