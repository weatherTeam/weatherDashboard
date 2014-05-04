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
		String year = key.toString();
		
		String[] vals = value.toString().split(",");
		String lat = vals[0];
		String lon = vals[1];
		String month = vals[2];
		String anomaly = vals[3];
		
			
		//output.collect(new Text(station), new Text(year+","+month+","+value));
		output.collect(new Text(year), new Text(month+"\t"+lat+"\t"+lon+"\t"+anomaly));

	}
}