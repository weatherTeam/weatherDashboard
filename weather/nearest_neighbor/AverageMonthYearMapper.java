package weather.nearest_neighbor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class AverageMonthYearMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
	
	private static Integer referenceYear;
	private static String tempPrec;
	private static String stationYearMonthPeriod;
	
	public void configure(JobConf job) {
		referenceYear = Integer.parseInt(job.get("referenceYear"));
	}

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String[] lineArray = value.toString().split("\\s+");
		tempPrec = lineArray[1];
		stationYearMonthPeriod = lineArray[0];
		
		if (stationYearMonthPeriod.substring(6,10).equals(referenceYear.toString())){
			output.collect(new Text(stationYearMonthPeriod), new Text(tempPrec));
		}
	}
}
