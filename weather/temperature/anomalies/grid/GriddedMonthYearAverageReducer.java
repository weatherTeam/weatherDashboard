package weather.temperature.anomalies.grid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GriddedMonthYearAverageReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		HashMap<String,Integer> monthAverage = new HashMap<String, Integer>();
		HashMap<String,Integer> nbRecords = new HashMap<String, Integer>();
		HashMap<String,Integer> maxMonthAverage = new HashMap<String, Integer>();
		HashMap<String,Integer> minMonthAverage = new HashMap<String, Integer>();

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			String year = value[1];
			String month = value[0];
			int temperature = Integer.parseInt(value[2]);
			int maxTemperature = Integer.parseInt(value[3]);
			int minTemperature = Integer.parseInt(value[4]);
			
			if (!monthAverage.containsKey(month+","+year)) {
				monthAverage.put(month+","+year, 0);
				nbRecords.put(month+","+year, 0);
				maxMonthAverage.put(month+","+year, 0);
				minMonthAverage.put(month+","+year, 0);
			}
			nbRecords.put(month+","+year,nbRecords.get(month+","+year) + 1);
			monthAverage.put(month+","+year,monthAverage.get(month+","+year) + temperature);
			maxMonthAverage.put(month+","+year,maxMonthAverage.get(month+","+year) + maxTemperature);
			minMonthAverage.put(month+","+year,minMonthAverage.get(month+","+year) + minTemperature);

		}
		for (String monthyear : monthAverage.keySet()) {
			if(nbRecords.get(monthyear)>0) {
				int avg = monthAverage.get(monthyear)/nbRecords.get(monthyear);
				int avgMax = maxMonthAverage.get(monthyear)/nbRecords.get(monthyear);
				int avgMin = minMonthAverage.get(monthyear)/nbRecords.get(monthyear);
	
				output.collect(key, new Text(monthyear+","+avg+","+avgMax+","+avgMin));
			}
		}
		
	}
}