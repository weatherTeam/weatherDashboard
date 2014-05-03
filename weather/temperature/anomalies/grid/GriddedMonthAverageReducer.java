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
		
		HashMap<String,Integer> monthAverage = new HashMap<String, Integer>();
		HashMap<String,Integer> nbRecords = new HashMap<String, Integer>();

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			String month = value[0];
			int temperature = Integer.parseInt(value[1]);						
			if (!monthAverage.containsKey(month)) {
				monthAverage.put(month, 0);
				nbRecords.put(month, 0);
			}
			monthAverage.put(month,monthAverage.get(month) + temperature);
			nbRecords.put(month,nbRecords.get(month) + 1);

		}
		for (String month : monthAverage.keySet()) {
			int avg = monthAverage.get(month)/nbRecords.get(month);
			output.collect(key, new Text(month+","+avg));
		}
	}
}