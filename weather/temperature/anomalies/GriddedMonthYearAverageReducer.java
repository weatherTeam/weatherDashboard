package weather.temperature.anomalies.grid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GriddedMonthYearAverageReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	
	private static int timeGranularity;

	public void configure(JobConf job) {
		timeGranularity = Integer.parseInt(job.get("timeGranularity"));
	}
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		HashMap<String,Integer> monthAverage = new HashMap<String, Integer>();
		HashMap<String,Integer> nbRecords = new HashMap<String, Integer>();
		HashMap<String,Integer> maxMonthAverage = new HashMap<String, Integer>();
		HashMap<String,Integer> minMonthAverage = new HashMap<String, Integer>();

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			String year = value[3];
			String month = value[4];
			String time = year+","+month;
			if (timeGranularity == 1) {
				time = year+","+ month + "," + value[5];
			}
			int temperature = Integer.parseInt(value[0]);
			int maxTemperature = Integer.parseInt(value[1]);
			int minTemperature = Integer.parseInt(value[2]);
			
			if (!monthAverage.containsKey(time)) {
				monthAverage.put(time, 0);
				nbRecords.put(time, 0);
				maxMonthAverage.put(time, 0);
				minMonthAverage.put(time, 0);
			}
			nbRecords.put(time,nbRecords.get(time) + 1);
			monthAverage.put(time,monthAverage.get(time) + temperature);
			maxMonthAverage.put(time,maxMonthAverage.get(time) + maxTemperature);
			minMonthAverage.put(time,minMonthAverage.get(time) + minTemperature);

		}
		for (String time : monthAverage.keySet()) {
			if(nbRecords.get(time)>0) {
				int avg = monthAverage.get(time)/nbRecords.get(time);
				int avgMax = maxMonthAverage.get(time)/nbRecords.get(time);
				int avgMin = minMonthAverage.get(time)/nbRecords.get(time);
	
				output.collect(key, new Text(avg+","+avgMax+","+avgMin+","+time));
			}
		}
		
	}
}