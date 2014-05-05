package weather.temperature.anomalies.grid;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StationsMonthYearAverageReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	
	private static int timeGranularity;
	
	public void configure(JobConf job) {
		timeGranularity = Integer.parseInt(job.get("timeGranularity"));
	}
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		//HashMap<String,Integer> monthMax = new HashMap<String, Integer>();
		// HashMap<String,Integer> nbRecords = new HashMap<String, Integer>();

		String[] keyVals = key.toString().split(",");
		String coord = keyVals[0];
		String year = keyVals[1];
		String month = keyVals[2];
		String time = year+","+month;
		if (timeGranularity==1) {
			time = year+","+month+","+keyVals[3];
		}
		
		int temperature = 0;
		int maxTemp = -99999;
		int minTemp = 99999;
		int nbRec = 0;
		
		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			//String year = value[0];
			//String month = value[1];

			//int temperature = Integer.parseInt(value[2]);
			int temp = Integer.parseInt(value[0]);

			if (temp > maxTemp) {
				maxTemp = temp;
			}
			
			if (temp < minTemp) {
				minTemp = temp;
			}
			
			temperature += temp;
			nbRec++;
			/*
			if (!monthAverage.containsKey(month+","+year)) {
				monthAverage.put(month+","+year, 0);
				nbRecords.put(month+","+year, 0);
			}
			monthAverage.put(month+","+year,monthAverage.get(month+","+year) + temperature);
			nbRecords.put(month+","+year,monthAverage.get(month+","+year) + 1);
			*/

		}
		int average = temperature/nbRec;

		output.collect(new Text(coord), new Text(average+","+maxTemp+","+minTemp+","+time));
		/*for (String monthyear : monthAverage.keySet()) {
			int avg = monthAverage.get(monthyear)/nbRecords.get(monthyear);
			output.collect(key, new Text(monthyear+","+avg));
		}*/
	}
}