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

public class GriddedMonthAverageReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private static int timeGranularity;

	public void configure(JobConf job) {
		timeGranularity = Integer.parseInt(job.get("timeGranularity"));
	}

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		HashMap<String, Integer> monthAverage = new HashMap<String, Integer>();
		HashMap<String, Integer> centileHighAverage = new HashMap<String, Integer>();
		HashMap<String, Integer> centileLowAverage = new HashMap<String, Integer>();
		HashMap<String, Integer> nbRecords = new HashMap<String, Integer>();
		HashMap<String, Integer> monthAverageMin = new HashMap<String, Integer>();
		HashMap<String, Integer> maxCentileHighAverage = new HashMap<String, Integer>();
		HashMap<String, Integer> maxCentileLowAverage = new HashMap<String, Integer>();
		HashMap<String, Integer> monthAverageMax = new HashMap<String, Integer>();
		HashMap<String, Integer> minCentileHighAverage = new HashMap<String, Integer>();
		HashMap<String, Integer> minCentileLowAverage = new HashMap<String, Integer>();

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			String month = value[9];
			String time = month;
			if (timeGranularity == 1) {
				time = month + "," + value[10];
			}
			int temperature = Integer.parseInt(value[0]);
			int centileHigh = Integer.parseInt(value[1]);
			int centileLow = Integer.parseInt(value[2]);
			int temperatureMax = Integer.parseInt(value[3]);
			int maxCentileHigh = Integer.parseInt(value[4]);
			int maxCentileLow = Integer.parseInt(value[5]);
			int temperatureMin = Integer.parseInt(value[6]);
			int minCentileHigh = Integer.parseInt(value[7]);
			int minCentileLow = Integer.parseInt(value[8]);

			if (!monthAverage.containsKey(time)) {
				monthAverage.put(time, 0);
				centileHighAverage.put(time, 0);
				centileLowAverage.put(time, 0);
				nbRecords.put(time, 0);
				monthAverageMin.put(time, 0);
				maxCentileHighAverage.put(time, 0);
				maxCentileLowAverage.put(time, 0);
				monthAverageMax.put(time, 0);
				minCentileHighAverage.put(time, 0);
				minCentileLowAverage.put(time, 0);

			}
			nbRecords.put(time, nbRecords.get(time) + 1);
			monthAverage.put(time, monthAverage.get(time) + temperature);
			centileHighAverage.put(time, centileHighAverage.get(time) + centileHigh);
			centileLowAverage.put(time, centileLowAverage.get(time) + centileLow);
			
			monthAverageMin.put(time, monthAverageMin.get(time)
					+ temperatureMin);
			maxCentileHighAverage.put(time, maxCentileHighAverage.get(time) + maxCentileHigh);
			maxCentileLowAverage.put(time, maxCentileLowAverage.get(time) + maxCentileLow);
			monthAverageMax.put(time, monthAverageMax.get(time)
					+ temperatureMax);
			minCentileHighAverage.put(time, minCentileHighAverage.get(time) + minCentileHigh);
			minCentileLowAverage.put(time, minCentileLowAverage.get(time) + minCentileLow);

		}
		for (String time : monthAverage.keySet()) {
			if (nbRecords.get(time) > 0) {
				int avg = monthAverage.get(time) / nbRecords.get(time);
				int centileHigh = centileHighAverage.get(time) / nbRecords.get(time);
				int centileLow = centileLowAverage.get(time) / nbRecords.get(time);
				int avgMax = monthAverageMax.get(time) / nbRecords.get(time);
				int maxCentileHigh = maxCentileHighAverage.get(time) / nbRecords.get(time);
				int maxCentileLow = maxCentileLowAverage.get(time) / nbRecords.get(time);
				int avgMin = monthAverageMin.get(time) / nbRecords.get(time);
				int minCentileHigh = minCentileHighAverage.get(time) / nbRecords.get(time);
				int minCentileLow = minCentileLowAverage.get(time) / nbRecords.get(time);
				output.collect(key, new Text(avg+","+centileHigh+","+centileLow+","+avgMax+","+maxCentileHigh+","+maxCentileLow+","+avgMin+","+minCentileHigh+","+minCentileLow+","+time));
			}
		}
	}
}