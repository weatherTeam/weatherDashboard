package ch.epfl.data.bigdata.weather.temperature.anomalies;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TemperatureAnomaliesReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		int temperature = 0;
		int averageTemperature = 0;

		while (values.hasNext()) {
			
			String val = values.next().toString();
			System.out.println(val);
			if(val.substring(0,1).equals("$")) {
				averageTemperature = Integer.parseInt(val.substring(1));
			} else {
				temperature = Integer.parseInt(val.substring(1));
			}
		}
		System.out.println(temperature +" - "+averageTemperature);
		int temperatureAnomaly = temperature-averageTemperature;
		output.collect(key, new Text(temperatureAnomaly+""));
	}
}