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
		HashMap<String, Integer> nbRecords = new HashMap<String, Integer>();
		HashMap<String, Integer> monthAverageMin = new HashMap<String, Integer>();
		HashMap<String, Integer> monthAverageMax = new HashMap<String, Integer>();

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			String month = value[3];
			String time = month;
			if (timeGranularity == 1) {
				time = month + "," + value[4];
			}
			int temperature = Integer.parseInt(value[0]);
			int temperatureMax = Integer.parseInt(value[1]);
			int temperatureMin = Integer.parseInt(value[2]);

			if (!monthAverage.containsKey(time)) {
				monthAverage.put(time, 0);
				nbRecords.put(time, 0);
				monthAverageMin.put(time, 0);
				monthAverageMax.put(time, 0);

			}
			nbRecords.put(time, nbRecords.get(time) + 1);
			monthAverage.put(time, monthAverage.get(time) + temperature);
			monthAverageMin.put(time, monthAverageMin.get(time)
					+ temperatureMin);
			monthAverageMax.put(time, monthAverageMax.get(time)
					+ temperatureMax);

		}
		for (String time : monthAverage.keySet()) {
			if (nbRecords.get(time) > 0) {
				int avg = monthAverage.get(time) / nbRecords.get(time);
				int avgMax = monthAverageMax.get(time) / nbRecords.get(time);
				int avgMin = monthAverageMin.get(time) / nbRecords.get(time);
				output.collect(key, new Text(avg + "," + avgMax + "," + avgMin+","+time));
			}
		}
	}
}