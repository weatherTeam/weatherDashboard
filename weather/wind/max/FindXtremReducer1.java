package weather.wind.max;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FindXtremReducer1 extends MapReduceBase implements
		Reducer<IntWritable, Text, IntWritable, Text>
{

	private static final int WIND_TRESHOLD = 250;

	@Override
	public void reduce(IntWritable inputKey, Iterator<Text> inputValue,
			OutputCollector<IntWritable, Text> output, Reporter arg3)
			throws IOException
	{

		// input = id + date + geoloc + wind + periodQuantityInHoursPrecipitation + millimetersPrecipitation

		final ArrayList<String> xtremWindInfo = new ArrayList<String>();

		while (inputValue.hasNext())
		{
			String infoString = inputValue.next().toString();
			int wind = Integer.parseInt(infoString.substring(36, 40));

			if (wind > WIND_TRESHOLD)
				xtremWindInfo.add(infoString);

		}

		for (int i = 0; i < xtremWindInfo.size(); ++i)
		{
			IntWritable outputKey = new IntWritable( Integer.parseInt(xtremWindInfo.get(i).substring(11, 19)));
			Text outputValue = new Text(xtremWindInfo.get(i));
			output.collect(outputKey, outputValue);
		}
	}
}
