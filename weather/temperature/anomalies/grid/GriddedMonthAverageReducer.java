package weather.temperature.anomalies.grid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GriddedMonthAverageReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		HashMap<String, Integer> monthAverage = new HashMap<String, Integer>();
		HashMap<String, Integer> nbRecords = new HashMap<String, Integer>();
		HashMap<String, Integer> monthAverageMin = new HashMap<String, Integer>();
		HashMap<String, Integer> monthAverageMax = new HashMap<String, Integer>();

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			String month = value[0];
			int temperature = Integer.parseInt(value[1]);
			int temperatureMax = Integer.parseInt(value[2]);
			int temperatureMin = Integer.parseInt(value[3]);

			if (!monthAverage.containsKey(month)) {
				monthAverage.put(month, 0);
				nbRecords.put(month, 0);
				monthAverageMin.put(month, 0);
				monthAverageMax.put(month, 0);

			}
			nbRecords.put(month, nbRecords.get(month) + 1);
			monthAverage.put(month, monthAverage.get(month) + temperature);
			monthAverageMin.put(month, monthAverageMin.get(month)
					+ temperatureMin);
			monthAverageMax.put(month, monthAverageMax.get(month)
					+ temperatureMax);

		}
		for (String month : monthAverage.keySet()) {
			if (nbRecords.get(month) > 0) {
				int avg = monthAverage.get(month) / nbRecords.get(month);
				int avgMax = monthAverageMax.get(month) / nbRecords.get(month);
				int avgMin = monthAverageMin.get(month) / nbRecords.get(month);
				output.collect(key, new Text(month + "," + avg + "," + avgMax
						+ "," + avgMin));
			}
		}
	}
}