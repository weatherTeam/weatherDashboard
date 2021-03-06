package weather.temperature;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/*
 * Copyright (c) Aubry Cholleton
 */

public class StationsReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private static int timeGranularity;

	public void configure(JobConf job) {
		timeGranularity = Integer.parseInt(job.get("timeGranularity"));
	}

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		String[] keyVals = key.toString().split(",");
		String coord = keyVals[0];
		String year = keyVals[1];
		String month = keyVals[2];
		String time = year + "," + month;
		if (timeGranularity == 1) {
			time = year + "," + month + "," + keyVals[3];
		}

		int val = 0;
		int maxValue = -99999;
		int minValue = 99999;
		int nbRec = 0;

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");

			int dataValue = Integer.parseInt(value[0]);

			if (dataValue > maxValue) {
				maxValue = dataValue;
			}

			if (dataValue < minValue) {
				minValue = dataValue;
			}

			val += dataValue;
			nbRec++;

		}
		int average = val / nbRec;

		output.collect(new Text(coord), new Text(average + "," + maxValue + ","
				+ minValue + "," + time));
	}
}