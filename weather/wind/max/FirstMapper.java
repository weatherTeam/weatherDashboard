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
	@Override
	public void map(LongWritable inputKey, Text inputValue,
			OutputCollector<IntWritable, Text> output, Reporter arg3) throws IOException
	{
		String[] dateAndWind = inputValue.toString().split(",");
		String month = (dateAndWind[0].substring(4, 5));
		
		IntWritable outputKey = new IntWritable(Integer.parseInt(month));
		IntWritable outputValue = new IntWritable(Integer.parseInt(dateAndWind[1]));
		
		output.collect(outputKey, new Text(dateAndWind[0]+","+outputValue));
		
	}
}