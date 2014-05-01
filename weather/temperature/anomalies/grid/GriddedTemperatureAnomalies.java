package weather.temperature.anomalies.grid;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;


public class GriddedTemperatureAnomalies {
	static final boolean PROD = true;
	public static void main(String[] args) throws IOException {
		if (args.length != 8) {
			System.err
					.println("Usage: Temperature anomalies <input path> <output path> <x step of the grid> <y step of the grid> <first year of reference period> <last year of reference period> <first year of period to analyse> <last year of period to analyse>");
			System.exit(-1);
		}
		
		Path tmp1 = new Path("tmp1");
		Path tmp2 = new Path("tmp2");
		Path rawOutput = new Path("rawOutput");
		String firstRefYear = args[4];
		String lastRefYear = args[5];
		String firstYear = args[6];
		String lastYear = args[7];
		String xStep = args[2];
		String yStep = args[3];
		
		if (PROD) {
			tmp1 = new Path("/team11/tempAnom/tmp1");
			tmp2 = new Path("/team11/tempAnom/tmp2");
			rawOutput = new Path("/team11/tempAnom/rawOutput");
		}
		

		JobConf avgMonthTemp = new JobConf(GriddedTemperatureAnomalies.class);
		avgMonthTemp.setJobName("AverageMonthTemperature");
		avgMonthTemp.set("firstYear",firstRefYear);
		avgMonthTemp.set("lastYear", lastRefYear);
		avgMonthTemp.set("xStep",xStep);
		avgMonthTemp.set("yStep", yStep);
		FileInputFormat.addInputPath(avgMonthTemp, new Path(args[0]));
		FileOutputFormat.setOutputPath(avgMonthTemp, tmp1);

		avgMonthTemp.setMapperClass(AverageMonthTemperatureMapper.class);
		avgMonthTemp.setReducerClass(AverageMonthTemperatureReducer.class);
		avgMonthTemp.setOutputKeyClass(Text.class);
		avgMonthTemp.setOutputValueClass(IntWritable.class);
		
		avgMonthTemp.setNumMapTasks(20);
		avgMonthTemp.setNumReduceTasks(20);
		JobClient.runJob(avgMonthTemp);

		JobConf avgMonthYearTemp = new JobConf(GriddedTemperatureAnomalies.class);
		avgMonthYearTemp.setJobName("AverageMonthYearTemperature");
		avgMonthYearTemp.set("firstYear", firstYear);
		avgMonthYearTemp.set("lastYear", lastYear);
		avgMonthYearTemp.set("xStep",xStep);
		avgMonthYearTemp.set("yStep", yStep);
		FileInputFormat.addInputPath(avgMonthYearTemp, new Path(args[0]));
		FileOutputFormat.setOutputPath(avgMonthYearTemp, tmp2);

		avgMonthYearTemp
				.setMapperClass(AverageMonthYearTemperatureMapper.class);
		avgMonthYearTemp
				.setReducerClass(AverageMonthYearTemperatureReducer.class);
		avgMonthYearTemp.setOutputKeyClass(Text.class);
		avgMonthYearTemp.setOutputValueClass(IntWritable.class);
		
		avgMonthYearTemp.setNumMapTasks(20);
		avgMonthYearTemp.setNumReduceTasks(20);
		JobClient.runJob(avgMonthYearTemp);

		JobConf temperatureAnomalies = new JobConf(GriddedTemperatureAnomalies.class);
		temperatureAnomalies.setJobName("temperatureAnomalies");
		temperatureAnomalies.set("firstYear", firstYear);
		temperatureAnomalies.set("lastYear", lastYear);
		MultipleInputs.addInputPath(temperatureAnomalies, tmp2,
				KeyValueTextInputFormat.class);
		MultipleInputs.addInputPath(temperatureAnomalies, tmp1,
				KeyValueTextInputFormat.class);

		FileOutputFormat.setOutputPath(temperatureAnomalies, rawOutput);

		temperatureAnomalies.setMapperClass(TemperatureAnomaliesMapper.class);
		temperatureAnomalies.setReducerClass(TemperatureAnomaliesReducer.class);
		temperatureAnomalies.setOutputKeyClass(Text.class);
		temperatureAnomalies.setOutputValueClass(Text.class);

		
		temperatureAnomalies.setNumMapTasks(20);
		temperatureAnomalies.setNumReduceTasks(20);
		
		JobClient.runJob(temperatureAnomalies);

		JobConf OutputTemperatureAnomalies = new JobConf(
				GriddedTemperatureAnomalies.class);
		OutputTemperatureAnomalies.setJobName("OutputTemperatureAnomalies");

		FileInputFormat.addInputPath(OutputTemperatureAnomalies, rawOutput);
		OutputTemperatureAnomalies
				.setInputFormat(KeyValueTextInputFormat.class);

		
		
        class OneFilePerKeyOutput extends MultipleTextOutputFormat<Text, Text> {
            protected String generateFileNameForKeyValue(Text key, Text value,String name) {
                    return key.toString();
            }
        }
        OutputTemperatureAnomalies.setOutputFormat(OneFilePerKeyOutput.class);
		FileOutputFormat.setOutputPath(OutputTemperatureAnomalies, new Path(
				args[1]));

		OutputTemperatureAnomalies
				.setMapperClass(OutputTemperatureAnomaliesMapper.class);
		OutputTemperatureAnomalies
				.setReducerClass(OutputTemperatureAnomaliesReducer.class);
		OutputTemperatureAnomalies.setOutputKeyClass(Text.class);
		OutputTemperatureAnomalies.setOutputValueClass(Text.class);
		
		
		OutputTemperatureAnomalies.setNumMapTasks(20);
		OutputTemperatureAnomalies.setNumReduceTasks(20);
		JobClient.runJob(OutputTemperatureAnomalies);
	}
}
