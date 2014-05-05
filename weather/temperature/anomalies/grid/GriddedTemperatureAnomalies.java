package weather.temperature.anomalies.grid;

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


public class GriddedTemperatureAnomalies {
	static final boolean PROD = false;
	public static void main(String[] args) throws IOException {
		if (args.length != 9) {
			System.err
					.println("Usage: Temperature anomalies <input path> <output path> <temporal granularity> <x step of the grid> <y step of the grid> <first year of reference period> <last year of reference period> <first year of period to analyse> <last year of period to analyse>");
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
			stationsMonthAveragePath = new Path("/team11/tempAnom/stationsMonthAverage");
			stationsMonthYearAveragePath = new Path("/team11/tempAnom/stationsMonthYearAverage");
			griddedMonthAveragePath = new Path("/team11/tempAnom/griddedMonthAverage");
			griddedMonthYearAveragePath = new Path("/team11/tempAnom/griddedMonthYearAverage");
			rawOutputPath = new Path("/team11/tempAnom/rawOutput");
			outputPath = new Path("/team11/tempAnom/output");
		}
		
		class OneFilePerKeyOutput extends MultipleTextOutputFormat<Text, Text> {
            protected String generateFileNameForKeyValue(Text key, Text value,String name) {
                    return key.toString();
            }
        }
		
		
		/*
		 * 
		 */
		
		JobConf stationsMonthAverage = new JobConf(GriddedTemperatureAnomalies.class);
		stationsMonthAverage.setJobName("StationsAverageMonthTemperature");
		stationsMonthAverage.set("firstYear",firstRefYear);
		stationsMonthAverage.set("lastYear", lastRefYear);
		stationsMonthAverage.set("timeGranularity", timeGranularity);
		FileInputFormat.addInputPath(stationsMonthAverage, new Path(args[0]));
		FileOutputFormat.setOutputPath(stationsMonthAverage, stationsMonthAveragePath);

		stationsMonthAverage.setMapperClass(StationsMonthAverageMapper.class);
		stationsMonthAverage.setReducerClass(StationsMonthAverageReducer.class);
		stationsMonthAverage.setOutputKeyClass(Text.class);
		stationsMonthAverage.setOutputValueClass(Text.class);
		//stationsMonthAverage.setOutputFormat(OneFilePerKeyOutput.class);
		
		stationsMonthAverage.setNumMapTasks(80);
		stationsMonthAverage.setNumReduceTasks(80);
		
		JobClient.runJob(stationsMonthAverage);
		

		
		JobConf stationsMonthYearAverage = new JobConf(GriddedTemperatureAnomalies.class);
		stationsMonthYearAverage.setJobName("StationsAverageMonthYearTemperature");
		stationsMonthYearAverage.set("firstYear",firstYear);
		stationsMonthYearAverage.set("lastYear", lastYear);
		stationsMonthYearAverage.set("timeGranularity", timeGranularity);
		FileInputFormat.addInputPath(stationsMonthYearAverage, new Path(args[0]));
		FileOutputFormat.setOutputPath(stationsMonthYearAverage, stationsMonthYearAveragePath);

		stationsMonthYearAverage.setMapperClass(StationsMonthYearAverageMapper.class);
		stationsMonthYearAverage.setReducerClass(StationsMonthYearAverageReducer.class);
		stationsMonthYearAverage.setOutputKeyClass(Text.class);
		stationsMonthYearAverage.setOutputValueClass(Text.class);
		//stationsMonthYearAverage.setOutputFormat(OneFilePerKeyOutput.class);
		
		stationsMonthYearAverage.setNumMapTasks(80);
		stationsMonthYearAverage.setNumReduceTasks(80);
		
		JobClient.runJob(stationsMonthYearAverage);


		
		JobConf griddedMonthAverage = new JobConf(GriddedTemperatureAnomalies.class);
		griddedMonthAverage.setJobName("GriddedAverageMonthTemperature");
		griddedMonthAverage.set("xStep",xStep);
		griddedMonthAverage.set("yStep", yStep);
		griddedMonthAverage.set("timeGranularity", timeGranularity);
		FileInputFormat.addInputPath(griddedMonthAverage, stationsMonthAveragePath);
		FileOutputFormat.setOutputPath(griddedMonthAverage, griddedMonthAveragePath);

		griddedMonthAverage.setMapperClass(GriddedMonthAverageMapper.class);
		griddedMonthAverage.setReducerClass(GriddedMonthAverageReducer.class);
		griddedMonthAverage.setOutputKeyClass(Text.class);
		griddedMonthAverage.setOutputValueClass(Text.class);
		//griddedMonthAverage.setOutputFormat(OneFilePerKeyOutput.class);
		griddedMonthAverage
		.setInputFormat(KeyValueTextInputFormat.class);
		
		griddedMonthAverage.setNumMapTasks(80);
		griddedMonthAverage.setNumReduceTasks(80);
		
		JobClient.runJob(griddedMonthAverage);
		

		
		JobConf griddedMonthYearAverage = new JobConf(GriddedTemperatureAnomalies.class);
		griddedMonthYearAverage.setJobName("GriddedAverageMonthYearTemperature");
		griddedMonthYearAverage.set("xStep",xStep);
		griddedMonthYearAverage.set("yStep", yStep);
		griddedMonthYearAverage.set("timeGranularity", timeGranularity);
		FileInputFormat.addInputPath(griddedMonthYearAverage, stationsMonthYearAveragePath);
		FileOutputFormat.setOutputPath(griddedMonthYearAverage, griddedMonthYearAveragePath);

		griddedMonthYearAverage.setMapperClass(GriddedMonthYearAverageMapper.class);
		griddedMonthYearAverage.setReducerClass(GriddedMonthYearAverageReducer.class);
		griddedMonthYearAverage.setOutputKeyClass(Text.class);
		griddedMonthYearAverage.setOutputValueClass(Text.class);
		//griddedMonthYearAverage.setOutputFormat(OneFilePerKeyOutput.class);
		griddedMonthYearAverage
		.setInputFormat(KeyValueTextInputFormat.class);
		
		griddedMonthYearAverage.setNumMapTasks(80);
		griddedMonthYearAverage.setNumReduceTasks(80);
		
		JobClient.runJob(griddedMonthYearAverage);
		


		JobConf temperatureAnomalies = new JobConf(GriddedTemperatureAnomalies.class);
		temperatureAnomalies.setJobName("temperatureAnomalies");
		temperatureAnomalies.set("firstYear",firstYear);
		temperatureAnomalies.set("lastYear", lastYear);
		temperatureAnomalies.set("timeGranularity", timeGranularity);
		MultipleInputs.addInputPath(temperatureAnomalies, griddedMonthAveragePath,
				KeyValueTextInputFormat.class);
		MultipleInputs.addInputPath(temperatureAnomalies, griddedMonthYearAveragePath,
				KeyValueTextInputFormat.class);

		FileOutputFormat.setOutputPath(temperatureAnomalies, rawOutputPath);

		temperatureAnomalies.setMapperClass(TemperatureAnomaliesMapper.class);
		temperatureAnomalies.setReducerClass(TemperatureAnomaliesReducer.class);
		temperatureAnomalies.setOutputKeyClass(Text.class);
		temperatureAnomalies.setOutputValueClass(Text.class);
		//temperatureAnomalies.setOutputFormat(OneSingleFileOutput.class);

		
		temperatureAnomalies.setNumMapTasks(80);
		temperatureAnomalies.setNumReduceTasks(80);
		
		JobClient.runJob(temperatureAnomalies);
		
		
		
		JobConf outputTemperatureAnomalies = new JobConf(GriddedTemperatureAnomalies.class);
		outputTemperatureAnomalies.setJobName("outputTemperatureAnomalies");
		outputTemperatureAnomalies.set("timeGranularity", timeGranularity);

		FileInputFormat.addInputPath(outputTemperatureAnomalies, rawOutputPath);
		FileOutputFormat.setOutputPath(outputTemperatureAnomalies, outputPath);

		outputTemperatureAnomalies.setMapperClass(OutputTemperatureAnomaliesMapper.class);
		outputTemperatureAnomalies.setReducerClass(OutputTemperatureAnomaliesReducer.class);
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
