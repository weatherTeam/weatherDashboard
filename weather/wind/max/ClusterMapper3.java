package weather.wind.max;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ClusterMapper3 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
{
	
	@Override
	public void map(LongWritable inputKey, Text inputValue,
			OutputCollector<IntWritable, Text> output, Reporter arg3) throws IOException
	{
		String[] split = inputValue.toString().split("\t");
		output.collect(new IntWritable(Integer.parseInt(split[0])), new Text(split[1]));
	}
}