package weather.temperature;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*
 * Copyright (c) Aubry Cholleton
 */

public class AnomaliesMapper extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {
	
	private static int firstYear;
	private static Integer lastYear;
	private static int timeGranularity;
	
	public void configure(JobConf job) {
		firstYear = Integer.parseInt(job.get("firstYear"));
		lastYear = Integer.parseInt(job.get("lastYear"));
		timeGranularity = Integer.parseInt(job.get("timeGranularity"));
	}
	public void map(Text key,  Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String[] values = value.toString().split(",");
		boolean reference = (values.length==10);
		if (timeGranularity == 1) {
		 reference = (values.length==11);
		}
		String coords = key.toString();

		if(reference) {

			String time = values[9];
			if (timeGranularity == 1) {
				time += "," + values[10];
			}

			for (int i = firstYear; i <= lastYear; i++) {
				output.collect(new Text(coords+","+i+","+time), new Text("$,"+value.toString()));
			}
			
		}
		else {
			String time = values[3]+","+values[4];
			if (timeGranularity == 1) {
				time += "," + values[5];
			}

			
			output.collect(new Text(coords+","+time), new Text("0,"+value.toString()));
		}

	}
}