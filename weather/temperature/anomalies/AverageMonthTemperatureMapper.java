package weather.temperature.anomalies;

/*
 * This mapper is inspired by a code example from
 * Hadoop: The Definitive Guide, Second
 * Edition, by Tom White. Copyright 2011 Tom White, 978-1-449-38973-4.
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AverageMonthTemperatureMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;

	private static int firstYear;
	private static Integer lastYear;
	
	public void configure(JobConf job) {
		firstYear = Integer.parseInt(job.get("firstYear"));
		lastYear = Integer.parseInt(job.get("lastYear"));
	}
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String line = value.toString();
		
		String year = line.substring(15, 19);
		int intYear = Integer.parseInt(year);
		
		if(intYear < firstYear || intYear>lastYear) 
			return;
		
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
			output.collect(new Text(station+month), new IntWritable(airTemperature));
		}
	}
}