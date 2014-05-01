package weather.temperature.anomalies.grid;

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
		
		String coords = keyString.substring(0,13);
		String lat = coords.substring(0,6);
		String lon = coords.substring(6,13);
		String year = keyString.substring(13,17);
		String month = keyString.substring(17,19);
		
			
		//output.collect(new Text(station), new Text(year+","+month+","+value));
		output.collect(new Text(month+""+year), new Text(lat+"\t"+lon+"\t"+value));

	}
}