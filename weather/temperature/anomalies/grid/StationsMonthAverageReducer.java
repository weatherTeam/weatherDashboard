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
	private static int timeGranularity;
	
	public void configure(JobConf job) {
		firstYear = Integer.parseInt(job.get("firstYear"));
		lastYear = Integer.parseInt(job.get("lastYear"));
		timeGranularity = Integer.parseInt(job.get("timeGranularity"));
	}
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		ArrayList<String> years = new ArrayList<String>();
		HashMap<String,Integer> monthAverage = new HashMap<String, Integer>();
		HashMap<String,Integer> nbRecords = new HashMap<String, Integer>();
		HashMap<String,HashMap<String,Integer>> monthYearMax = new HashMap<String, HashMap<String,Integer>>();
		HashMap<String,HashMap<String,Integer>> monthYearMin = new HashMap<String, HashMap<String,Integer>>();
		

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			String coord = value[0];
			String year = value[2];
			String month = value[3];
			int temperature = Integer.parseInt(value[1]);
			

			if (!years.contains(year)) {
				years.add(year);
			}
			
			String k = coord+","+month;
			if(timeGranularity == 1)
				k = coord+","+month+","+value[4];
							
			if (!monthAverage.containsKey(k)) {
				monthAverage.put(k, 0);
				nbRecords.put(k, 0);
				monthYearMax.put(k, new HashMap<String,Integer>());
				monthYearMin.put(k, new HashMap<String,Integer>());
			}

			if (!monthYearMax.get(k).containsKey(year)) {
				monthYearMax.get(k).put(year, -99999);
			}
			if (temperature > monthYearMax.get(k).get(year))
				monthYearMax.get(k).put(year, temperature);
			
			if (!monthYearMin.get(k).containsKey(year)) {
				monthYearMin.get(k).put(year, +99999);
			}
			if (temperature < monthYearMin.get(k).get(year))
				monthYearMin.get(k).put(year, temperature);
			
			
			monthAverage.put(k,monthAverage.get(k) + temperature);
			nbRecords.put(k,nbRecords.get(k) + 1);

		}
		System.out.println(years.size());
		
		// Here we check that the stations has enough data over
		// the reference period to be interesting
		//if (years.size() > (lastYear - firstYear)/4) {
		if (years.size() >= 2) {
			for (String k : monthAverage.keySet()) {
				String coord = k.split(",")[0];
				String month = k.split(",")[1];
				String time = month;
				if(timeGranularity == 1)
					time = month+","+k.split(",")[2];
				
				int maxAvg = 0;
				for (String l : monthYearMax.get(k).keySet()) {
					maxAvg+=monthYearMax.get(k).get(l);
				}
				maxAvg/=monthYearMax.get(k).size();
				
				int minAvg = 0;
				for (String l : monthYearMin.get(k).keySet()) {
					minAvg+=monthYearMin.get(k).get(l);
				}
				minAvg/=monthYearMin.get(k).size();
				
				int avg = monthAverage.get(k)/nbRecords.get(k);
				output.collect(new Text(coord), new Text(avg+","+maxAvg+","+minAvg+","+time));
			}
		}
	}
}