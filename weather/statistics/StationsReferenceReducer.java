package weather.statistics;

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
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/*
 * Copyright (c) Aubry Cholleton
 */
public class StationsReferenceReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	// private static int firstYear;
	// private static int lastYear;
	private static int timeGranularity;

	public void configure(JobConf job) {
		// firstYear = Integer.parseInt(job.get("firstYear"));
		// lastYear = Integer.parseInt(job.get("lastYear"));
		timeGranularity = Integer.parseInt(job.get("timeGranularity"));
	}

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		ArrayList<String> years = new ArrayList<String>();

		HashMap<String, HashMap<String, ArrayList<Double>>> dataValues = new HashMap<String, HashMap<String, ArrayList<Double>>>();

		while (values.hasNext()) {
			String[] value = values.next().toString().split(",");
			String coord = value[0];
			String year = value[2];
			String month = value[3];
			double temperature = Double.parseDouble(value[1]);

			if (!years.contains(year)) {
				years.add(year);
			}

			String k = coord + "," + month;

			if (timeGranularity == 1)
				k = coord + "," + month + "," + value[4];

			if (!dataValues.containsKey(k)) {
				dataValues.put(k, new HashMap<String, ArrayList<Double>>());
			}

			if (!dataValues.get(k).containsKey(year)) {
				dataValues.get(k).put(year, new ArrayList<Double>());
			}

			dataValues.get(k).get(year).add(temperature);

		}

		// Here we check that the stations has enough data over
		// the reference period to be interesting
		// if (years.size() > (lastYear - firstYear)/4) {
		if (years.size() >= 2) {

			for (String k : dataValues.keySet()) {
				String coord = k.split(",")[0];
				String month = k.split(",")[1];
				String time = month;
				if (timeGranularity == 1)
					time += "," + k.split(",")[2];

				DescriptiveStatistics avgT = new DescriptiveStatistics();
				DescriptiveStatistics avgMax = new DescriptiveStatistics();
				DescriptiveStatistics avgMin = new DescriptiveStatistics();

				for (String timeKey : dataValues.get(k).keySet()) {
					DescriptiveStatistics max = new DescriptiveStatistics();
					for (Double val : dataValues.get(k).get(timeKey)) {
						max.addValue(val);
					}
					avgT.addValue(max.getMean());
					avgMax.addValue(max.getMax());
					avgMin.addValue(max.getMin());
				}

				int avg = (int) avgT.getMean();
				int centileHigh = (int) avgT.getPercentile(93);
				int centileLow = (int) avgT.getPercentile(7);

				int maxAvg = (int) avgMax.getMean();
				int maxCentileHigh = (int) avgMax.getPercentile(93);
				int maxCentileLow = (int) avgMax.getPercentile(7);

				int minAvg = (int) avgMin.getMean();
				int minCentileHigh = (int) avgMin.getPercentile(93);
				int minCentileLow = (int) avgMin.getPercentile(7);

				output.collect(new Text(coord), new Text(avg + ","
						+ centileHigh + "," + centileLow + "," + maxAvg + ","
						+ maxCentileHigh + "," + maxCentileLow + "," + minAvg
						+ "," + minCentileHigh + "," + minCentileLow + ","
						+ time));
			}
		}
	}
}