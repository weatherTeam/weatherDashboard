package ch.epfl.data.bigdata.weather.temperature.anomalies;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TemperatureAnomaliesMapper extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {
	
	private static int firstYear;
	private static Integer lastYear;
	
	public void configure(JobConf job) {
		firstYear = Integer.parseInt(job.get("firstYear"));
		lastYear = Integer.parseInt(job.get("lastYear"));
	}
	public void map(Text key,  Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String keyString = key.toString();
		
		String station = keyString.substring(0,6);
		String month = keyString.substring(6,8);
		
		
		if(keyString.length() == 8) {
			for (int i = firstYear; i <= lastYear; i++) {
				output.collect(new Text(station+i+month), new Text("$"+value));
			}
			
		}
		else if (keyString.length() == 12) {
			String year = keyString.substring(8,12);
			
			output.collect(new Text(station+year+month), new Text("0"+value));
		}
		else {
			System.out.println("Wrong formatting");
		}
	}
}