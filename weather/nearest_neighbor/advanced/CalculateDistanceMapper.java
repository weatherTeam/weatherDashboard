package weather.nearest_neighbor.advanced;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CalculateDistanceMapper extends MapReduceBase implements 
	Mapper<LongWritable, Text, IntWritable, Text> {
	
    private String temp;
    private String date;
    private int year;
    private String month;

	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		String[] lineArray = value.toString().split("\\s+");
		date = lineArray[0];
		temp = lineArray[1];
		
		year = Integer.parseInt(date.substring(0,4));
		month = date.substring(4,6);
		
		output.collect(new IntWritable(year), new Text(month+";"+temp));
	}
}
