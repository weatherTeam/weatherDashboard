package weather.temperature.anomalies;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class OutputTemperatureAnomaliesMapper extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {
	
	public void map(Text key,  Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String keyString = key.toString();
		
		String station = keyString.substring(0,6);
		String month = keyString.substring(6,10);
		String year = keyString.substring(10,12);
		
			
		//output.collect(new Text(station), new Text(year+","+month+","+value));
		output.collect(new Text(year+","+month), new Text(station+","+value));

	}
}