package weather.nearest_neighbor.advanced;

import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CalculateDistanceMapper extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, Text> {
	
    private String temp;
    private String stationDate;
    private int year;
    private String month;
    private String station;

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] lineArray = value.toString().split("\\s+");
		stationDate = lineArray[0];
		temp = lineArray[1];
		
		station = stationDate.substring(0,6);
		year = Integer.parseInt(stationDate.substring(6,10));
		month = stationDate.substring(10,12);
		
		output.collect(new Text(station+year), new Text(month+";"+temp));
	}
}
