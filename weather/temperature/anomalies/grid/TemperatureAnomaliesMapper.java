package weather.temperature.anomalies.grid;

import java.io.IOException;
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
		
		String coords = keyString.substring(0,13);
		String month = keyString.substring(13,15);
		
		if(keyString.length() ==15) {
			for (int i = firstYear; i <= lastYear; i++) {
				output.collect(new Text(coords+i+month), new Text("$"+value));
			}
			
		}
		else if (keyString.length() == 19) {
			String year = keyString.substring(15,19);
			
			output.collect(new Text(coords+year+month), new Text("0"+value));
		}
		else {
			System.out.println("Wrong formatting");
		}
	}
}