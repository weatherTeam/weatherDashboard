package weather.temperature.anomalies.grid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StationsMonthAverageReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	
	private static int firstYear;
	private static int lastYear;
	
	public void configure(JobConf job) {
		firstYear = Integer.parseInt(job.get("firstYear"));
		lastYear = Integer.parseInt(job.get("lastYear"));
	}
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		ArrayList<String> years = new ArrayList<String>();
		HashMap<String,Integer> monthAverage = new HashMap<String, Integer>();
		HashMap<String,Integer> nbRecords = new HashMap<String, Integer>();
		

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			String coord = value[0];
			String year = value[1];
			String month = value[2];
			int temperature = Integer.parseInt(value[3]);			
			if (!years.contains(year)) {
				years.add(year);
			}
							
			if (!monthAverage.containsKey(month)) {
				monthAverage.put(coord+","+month, 0);
				nbRecords.put(coord+","+month, 0);
			}
			monthAverage.put(coord+","+month,monthAverage.get(coord+","+month) + temperature);
			nbRecords.put(coord+","+month,nbRecords.get(coord+","+month) + 1);

		}
		System.out.println(years.size());

		if (years.size() > (lastYear - firstYear)/2) {
			for (String k : monthAverage.keySet()) {
				String coord = k.split(",")[0];
				String month = k.split(",")[1];
				int avg = monthAverage.get(coord+","+month)/nbRecords.get(coord+","+month);
				output.collect(new Text(coord), new Text(month+","+avg));
			}
		}
	}
}