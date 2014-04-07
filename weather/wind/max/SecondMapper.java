package weather.wind.max;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// Mapper<inputKey, inputValue, outputKey, outputValue>

public class SecondMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
	@Override
	public void map(LongWritable inputKey, Text inputValue,
			OutputCollector<Text, Text> output, Reporter arg3) throws IOException
	{
		output.collect(new Text(""), inputValue);
	}
}