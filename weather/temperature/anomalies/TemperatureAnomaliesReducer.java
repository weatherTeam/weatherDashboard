package weather.temperature.anomalies.grid;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TemperatureAnomaliesReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	private static int timeGranularity;
	public void configure(JobConf job) {

		timeGranularity = Integer.parseInt(job.get("timeGranularity"));
	}
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		int temperature = 0;
		int centileHigh = 0;
		int centileLow = 0;
		int temperatureMax = 0;
		int maxCentileHigh = 0;
		int maxCentileLow = 0;
		int temperatureMin = 0;
		int minCentileHigh = 0;
		int minCentileLow = 0;
		int averageTemperature = 0;
		int averageMax = 0;
		int averageMin = 0;
		boolean refExists = false;
		boolean valExists = false;
		while (values.hasNext()) {
			
			String[] val = values.next().toString().split(",");
			System.out.println(val);
			if(val[0].equals("$")) {
				refExists = true;
				averageTemperature = Integer.parseInt(val[1]);
				centileHigh = Integer.parseInt(val[2]);
				centileLow = Integer.parseInt(val[3]);
				averageMax = Integer.parseInt(val[4]);
				maxCentileHigh = Integer.parseInt(val[5]);
				maxCentileLow = Integer.parseInt(val[6]);
				averageMin = Integer.parseInt(val[7]);
				minCentileHigh = Integer.parseInt(val[8]);
				minCentileLow = Integer.parseInt(val[9]);
			} else {
				valExists = true;
				temperature = Integer.parseInt(val[1]);
				temperatureMax = Integer.parseInt(val[2]);
				temperatureMin = Integer.parseInt(val[3]);
			}
		}
		if (!refExists || !valExists) {
			System.out.println("DO NOT EXISTS");
			return;
		}
		int temperatureAnomaly = temperature-averageTemperature;
		int maxAnomaly = temperatureMax-averageMax;
		int minAnomaly = temperatureMin-averageMin;
		String[] keyvals = key.toString().split(",");
		String lat = keyvals[0];
		String lon = keyvals[1];
		String year = keyvals[2];
		String month = keyvals[3];
		String time = month;
		String eventType = "NORMAL";
		if (timeGranularity == 1) {
			time += "," +  keyvals[4];
		}
		
		if (temperatureMax >= maxCentileHigh) {
			if (temperatureMin >= maxCentileLow)
				eventType = "VERYHOT";
			else eventType = "HOT";
		} else if (temperatureMin <= minCentileLow) {
			if (temperatureMax <= minCentileHigh)
				eventType = "VERYCOLD";
			else eventType = "COLD";
		} 

		/*
		if (timeGranularity == 1) {
			time += "," +  keyvals[4];
			if (temperatureMax > averageMax && temperatureMin > averageMin && temperature > averageTemperature) {
				eventType = "HOT";
			} else if (temperatureMax < averageMax && temperatureMin < averageMin && temperature < averageTemperature) {
				eventType = "COLD";
			}
		} else {
			if (temperature > ((averageMax+averageTemperature)/2)) {
				eventType = "HOT";
			} else if (temperature < ((averageMin+averageTemperature)/2)) {
				eventType = "COLD";
			}
		}
		*/
		
		//output.collect(new Text(lat+"\t"+lon+"\t"+year+"\t"+month), new Text(temperatureAnomaly+""));
		output.collect(new Text(year), new Text(lat+","+lon+","+temperatureAnomaly+","+maxAnomaly+","+minAnomaly+","+eventType+","+time));
	}
}