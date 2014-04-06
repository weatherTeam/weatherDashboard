package ch.epfl.data.bigdata.weather.temperature.anomalies;

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

public class TemperatureAnomalies {
	public static void main(String[] args) throws IOException {
		if (args.length != 6) {
			System.err
					.println("Usage: Temperature anomalies <input path> <output path> <first year of reference period> <last year of reference period> <first year of period to analyse> <last year of period to analyse>");
			System.exit(-1);
		}

		JobConf avgMonthTemp = new JobConf(TemperatureAnomalies.class);
		avgMonthTemp.setJobName("AverageMonthTemperature");
		avgMonthTemp.set("firstYear", args[2]);
		avgMonthTemp.set("lastYear", args[3]);
		FileInputFormat.addInputPath(avgMonthTemp, new Path(args[0]));
		FileOutputFormat.setOutputPath(avgMonthTemp, new Path("tmp1"));

		avgMonthTemp.setMapperClass(AverageMonthTemperatureMapper.class);
		avgMonthTemp.setReducerClass(AverageMonthTemperatureReducer.class);
		avgMonthTemp.setOutputKeyClass(Text.class);
		avgMonthTemp.setOutputValueClass(IntWritable.class);
		JobClient.runJob(avgMonthTemp);
		
		JobConf avgMonthYearTemp = new JobConf(TemperatureAnomalies.class);
		avgMonthYearTemp.setJobName("AverageMonthYearTemperature");
		avgMonthYearTemp.set("firstYear", args[4]);
		avgMonthYearTemp.set("lastYear", args[5]);
		FileInputFormat.addInputPath(avgMonthYearTemp, new Path(args[0]));
		FileOutputFormat.setOutputPath(avgMonthYearTemp, new Path("tmp2"));

		avgMonthYearTemp.setMapperClass(AverageMonthYearTemperatureMapper.class);
		avgMonthYearTemp.setReducerClass(AverageMonthYearTemperatureReducer.class);
		avgMonthYearTemp.setOutputKeyClass(Text.class);
		avgMonthYearTemp.setOutputValueClass(IntWritable.class);
		JobClient.runJob(avgMonthYearTemp);
		
		JobConf temperatureAnomalies = new JobConf(TemperatureAnomalies.class);
		temperatureAnomalies.setJobName("temperatureAnomalies");
		temperatureAnomalies.set("firstYear", args[4]);
		temperatureAnomalies.set("lastYear", args[5]);
		MultipleInputs.addInputPath(temperatureAnomalies, new Path("tmp2"),KeyValueTextInputFormat.class);
		MultipleInputs.addInputPath(temperatureAnomalies, new Path("tmp1"),KeyValueTextInputFormat.class);
		
		FileOutputFormat.setOutputPath(temperatureAnomalies, new Path(args[1]));

		temperatureAnomalies.setMapperClass(TemperatureAnomaliesMapper.class);
		temperatureAnomalies.setReducerClass(TemperatureAnomaliesReducer.class);
		temperatureAnomalies.setOutputKeyClass(Text.class);
		temperatureAnomalies.setOutputValueClass(Text.class);

		JobClient.runJob(temperatureAnomalies);
	}
}
