package weather.wind.max;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FindCenterMapper4 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
	
	@Override
	public void map(LongWritable inputKey, Text inputValue,
			OutputCollector<Text, Text> output, Reporter arg3) throws IOException
	{
		String[] split = inputValue.toString().split("\t");
		output.collect(new Text(split[0]), new Text(split[1]));
	}
}