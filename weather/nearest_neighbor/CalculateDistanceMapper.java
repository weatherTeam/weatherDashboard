package weather.nearest_neighbor;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// This mapper emits (station+year+month, period + temp + precipitation) 

public class CalculateDistanceMapper extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, Text> {
	
    private String[] values;
	private String temp;
    private String prec;
    private String stationDate;
    private int year;
    private String month;
    private String period;
    private String station;

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] lineArray = value.toString().split("\\s+");
		stationDate = lineArray[0];
		values = lineArray[1].split(";");
		temp = values[0];
		prec = values[1];
		station = stationDate.substring(0,6);
		year = Integer.parseInt(stationDate.substring(6,10));
		month = stationDate.substring(10,12);
		period = stationDate.substring(12,13);
		
		output.collect(new Text(station+year+month), new Text(period+":"+temp+";"+prec));
	}
}
