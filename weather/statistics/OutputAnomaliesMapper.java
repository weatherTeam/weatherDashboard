package weather.statistics;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class OutputAnomaliesMapper extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {
	private static int timeGranularity;
	public void configure(JobConf job) {

		timeGranularity = Integer.parseInt(job.get("timeGranularity"));
	}
	public void map(Text key,  Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String year = key.toString();
		
		String[] vals = value.toString().split(",");
		String lat = vals[0];
		String lon = vals[1]; 
		String anomaly = vals[2];
		String anomalyMax = vals[3];
		String anomalyMin = vals[4];
		String isExtreme = vals[5];
		String time = vals[6];
		if (timeGranularity == 1)
			time += vals[7];

		
			
		//output.collect(new Text(station), new Text(year+","+month+","+value));
		output.collect(new Text(year+time), new Text(lat+"\t"+lon+"\t"+anomaly+"\t"+anomalyMax+"\t"+anomalyMin+"\t"+isExtreme));

	}
}