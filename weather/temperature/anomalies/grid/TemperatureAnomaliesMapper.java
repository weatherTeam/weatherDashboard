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
		
		String[] values = value.toString().split(",");
		boolean reference = (values.length==4);
		String coords = key.toString();
		String month = values[0];
		
		if(reference) {
			String val = values[1];
			String valMax = values[2];
			String valMin = values[3];
			for (int i = firstYear; i <= lastYear; i++) {
				output.collect(new Text(coords+i+month), new Text("$,"+val+","+valMax+","+valMin));
			}
			
		}
		else {
			String year = values[1];
			String val = values[2];
			String valMax = values[3];
			String valMin = values[4];
			
			output.collect(new Text(coords+year+month), new Text("0,"+val+","+valMax+","+valMin));
		}

	}
}