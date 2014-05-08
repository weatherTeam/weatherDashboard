package weather.temperature;

/*
 * Copyright (c) Aubry Cholleton
 * 
 * This software can be used to analyze weather data
 * from NOAA dataset. It can aggregate data spatially (grid)
 * or/and temporally (month, day), compute maximums averages, standard deviation,
 * percentiles, and mainly compute anomalies over a given period with
 * respect to a given reference period. It can also use these statistics to extract extreme
 * events.
 */

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;


public class WeatherStatistics {
	static final boolean PROD = false;

	public static void main(String[] args) throws IOException {
		if (args.length != 9) {
			System.err
					.println("Usage: Anomalies <input path> <output path> <temporal granularity> <x step of the grid> <y step of the grid> <first year of reference period> <last year of reference period> <first year of period to analyse> <last year of period to analyse>");
			System.exit(-1);
		}

		Path stationsMonthAveragePath = new Path("stationsMonthAverage");
		Path stationsMonthYearAveragePath = new Path("stationsMonthYearAverage");
		Path griddedMonthAveragePath = new Path("griddedMonthAverage");
		Path griddedMonthYearAveragePath = new Path("griddedMonthYearAverage");
		Path rawOutputPath = new Path("rawOutput");
		Path outputPath = new Path("output");
		String firstRefYear = args[5];
		String lastRefYear = args[6];
		String firstYear = args[7];
		String lastYear = args[8];
		String xStep = args[3];
		String yStep = args[4];
		String timeGranularity = args[2];

		if (PROD) {
			stationsMonthAveragePath = new Path(
					"/team11/tempAnom/stationsMonthAverage");
			stationsMonthYearAveragePath = new Path(
					"/team11/tempAnom/stationsMonthYearAverage");
			griddedMonthAveragePath = new Path(
					"/team11/tempAnom/griddedMonthAverage");
			griddedMonthYearAveragePath = new Path(
					"/team11/tempAnom/griddedMonthYearAverage");
			rawOutputPath = new Path("/team11/tempAnom/rawOutput");
			outputPath = new Path("/team11/tempAnom/output");
		}

		class OneFilePerKeyOutput extends MultipleTextOutputFormat<Text, Text> {
			protected String generateFileNameForKeyValue(Text key, Text value,
					String name) {
				return key.toString();
			}
		}

		JobConf stationsMonthAverage = new JobConf(WeatherStatistics.class);
		stationsMonthAverage.setJobName("StationsAverageMonthTemperature");
		stationsMonthAverage.set("firstYear", firstRefYear);
		stationsMonthAverage.set("lastYear", lastRefYear);
		stationsMonthAverage.set("timeGranularity", timeGranularity);
		FileInputFormat.addInputPath(stationsMonthAverage, new Path(args[0]));
		FileOutputFormat.setOutputPath(stationsMonthAverage,
				stationsMonthAveragePath);

		stationsMonthAverage.setMapperClass(StationsReferenceMapper.class);
		stationsMonthAverage.setReducerClass(StationsReferenceReducer.class);
		stationsMonthAverage.setOutputKeyClass(Text.class);
		stationsMonthAverage.setOutputValueClass(Text.class);

		stationsMonthAverage.setNumMapTasks(80);
		stationsMonthAverage.setNumReduceTasks(80);

		JobClient.runJob(stationsMonthAverage);

		JobConf stationsMonthYearAverage = new JobConf(WeatherStatistics.class);
		stationsMonthYearAverage
				.setJobName("StationsAverageMonthYearTemperature");
		stationsMonthYearAverage.set("firstYear", firstYear);
		stationsMonthYearAverage.set("lastYear", lastYear);
		stationsMonthYearAverage.set("timeGranularity", timeGranularity);
		FileInputFormat.addInputPath(stationsMonthYearAverage,
				new Path(args[0]));
		FileOutputFormat.setOutputPath(stationsMonthYearAverage,
				stationsMonthYearAveragePath);

		stationsMonthYearAverage.setMapperClass(StationsMapper.class);
		stationsMonthYearAverage.setReducerClass(StationsReducer.class);
		stationsMonthYearAverage.setOutputKeyClass(Text.class);
		stationsMonthYearAverage.setOutputValueClass(Text.class);

		stationsMonthYearAverage.setNumMapTasks(80);
		stationsMonthYearAverage.setNumReduceTasks(80);

		JobClient.runJob(stationsMonthYearAverage);

		JobConf griddedMonthAverage = new JobConf(WeatherStatistics.class);
		griddedMonthAverage.setJobName("GriddedAverageMonthTemperature");
		griddedMonthAverage.set("xStep", xStep);
		griddedMonthAverage.set("yStep", yStep);
		griddedMonthAverage.set("timeGranularity", timeGranularity);
		FileInputFormat.addInputPath(griddedMonthAverage,
				stationsMonthAveragePath);
		FileOutputFormat.setOutputPath(griddedMonthAverage,
				griddedMonthAveragePath);

		griddedMonthAverage.setMapperClass(GriddedReferenceMapper.class);
		griddedMonthAverage.setReducerClass(GriddedReferenceReducer.class);
		griddedMonthAverage.setOutputKeyClass(Text.class);
		griddedMonthAverage.setOutputValueClass(Text.class);
		griddedMonthAverage.setInputFormat(KeyValueTextInputFormat.class);

		griddedMonthAverage.setNumMapTasks(80);
		griddedMonthAverage.setNumReduceTasks(80);

		JobClient.runJob(griddedMonthAverage);

		JobConf griddedMonthYearAverage = new JobConf(WeatherStatistics.class);
		griddedMonthYearAverage
				.setJobName("GriddedAverageMonthYearTemperature");
		griddedMonthYearAverage.set("xStep", xStep);
		griddedMonthYearAverage.set("yStep", yStep);
		griddedMonthYearAverage.set("timeGranularity", timeGranularity);
		FileInputFormat.addInputPath(griddedMonthYearAverage,
				stationsMonthYearAveragePath);
		FileOutputFormat.setOutputPath(griddedMonthYearAverage,
				griddedMonthYearAveragePath);

		griddedMonthYearAverage
				.setMapperClass(GriddedMapper.class);
		griddedMonthYearAverage
				.setReducerClass(GriddedReducer.class);
		griddedMonthYearAverage.setOutputKeyClass(Text.class);
		griddedMonthYearAverage.setOutputValueClass(Text.class);
		griddedMonthYearAverage.setInputFormat(KeyValueTextInputFormat.class);

		griddedMonthYearAverage.setNumMapTasks(80);
		griddedMonthYearAverage.setNumReduceTasks(80);

		JobClient.runJob(griddedMonthYearAverage);

		JobConf temperatureAnomalies = new JobConf(WeatherStatistics.class);
		temperatureAnomalies.setJobName("temperatureAnomalies");
		temperatureAnomalies.set("firstYear", firstYear);
		temperatureAnomalies.set("lastYear", lastYear);
		temperatureAnomalies.set("timeGranularity", timeGranularity);
		MultipleInputs.addInputPath(temperatureAnomalies,
				griddedMonthAveragePath, KeyValueTextInputFormat.class);
		MultipleInputs.addInputPath(temperatureAnomalies,
				griddedMonthYearAveragePath, KeyValueTextInputFormat.class);

		FileOutputFormat.setOutputPath(temperatureAnomalies, rawOutputPath);

		temperatureAnomalies.setMapperClass(AnomaliesMapper.class);
		temperatureAnomalies.setReducerClass(AnomaliesReducer.class);
		temperatureAnomalies.setOutputKeyClass(Text.class);
		temperatureAnomalies.setOutputValueClass(Text.class);

		temperatureAnomalies.setNumMapTasks(80);
		temperatureAnomalies.setNumReduceTasks(80);

		JobClient.runJob(temperatureAnomalies);

		JobConf outputTemperatureAnomalies = new JobConf(WeatherStatistics.class);
		outputTemperatureAnomalies.setJobName("outputTemperatureAnomalies");
		outputTemperatureAnomalies.set("timeGranularity", timeGranularity);

		FileInputFormat.addInputPath(outputTemperatureAnomalies, rawOutputPath);
		FileOutputFormat.setOutputPath(outputTemperatureAnomalies, outputPath);

		outputTemperatureAnomalies.setMapperClass(OutputAnomaliesMapper.class);
		outputTemperatureAnomalies
				.setReducerClass(OutputAnomaliesReducer.class);
		outputTemperatureAnomalies.setOutputKeyClass(Text.class);
		outputTemperatureAnomalies.setOutputValueClass(Text.class);
		outputTemperatureAnomalies.setOutputFormat(OneFilePerKeyOutput.class);
		outputTemperatureAnomalies
				.setInputFormat(KeyValueTextInputFormat.class);

		outputTemperatureAnomalies.setNumMapTasks(80);
		outputTemperatureAnomalies.setNumReduceTasks(80);

		JobClient.runJob(outputTemperatureAnomalies);

	}
}
