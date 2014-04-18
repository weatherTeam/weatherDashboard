package weather.nearest_neighbor.advanced;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AverageMonthMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;
	
	public void map(LongWritable key, Text value,
		OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
	
	String line = value.toString();
	
	String year = line.substring(15, 19);
	int intYear = Integer.parseInt(year);
	
	String station = line.substring(4, 10);
	String month = line.substring(19, 21);
	int airTemperature;
	if (line.charAt(87) == '+') {
		airTemperature = Integer.parseInt(line.substring(88, 92));
	} else {
		airTemperature = Integer.parseInt(line.substring(87, 92));
	}
	String quality = line.substring(92, 93);
	if (airTemperature != MISSING && quality.matches("[01459]")) {
		output.collect(new Text(station+intYear+month), new IntWritable(airTemperature));
	}
	}
}
