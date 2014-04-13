package weather.rain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RainAnomalies {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: CMD <input path> output Path>");
			System.exit(-1);
		}
		Job job = RainAnomalies.getJob(args[0], args[1]);
		job.waitForCompletion(true);
		System.exit(0);

	}

	private static Job getJob(String inputPath, String outputPath)
			throws IOException {
		Job job = Job.getInstance();
		job.setJarByClass(RainAnomalies.class);
		job.setJobName("RainAnomalies");

		job.setMapperClass(RainAnomaliesMapper1.class);
		job.setReducerClass(RainAnomaliesReducer1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputPath), true);
		if (inputPath.endsWith(".txt"))
			FileInputFormat.addInputPath(job, new Path(inputPath));
		else {
			FileStatus[] status_list = fs.listStatus(new Path(inputPath));
			if (status_list != null) {
				for (FileStatus status : status_list) {
					FileInputFormat.addInputPath(job, status.getPath());
				}
			}
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job;
	}

	public static class RainAnomaliesMapper1 extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void map(LongWritable inputKey, Text inputValue, Context context)
				throws IOException, InterruptedException {
			String line = inputValue.toString();
			String stationID = line.substring(4, 10);
			String date = line.substring(15, 23);
			int year = Integer.parseInt(date.substring(0, 4));
			// int month = Integer.parseInt(line.substring(20,22));
			// int day = Integer.parseInt(line.substring(22,24));

			int index = line.indexOf("ADDAA1");
			if (index != -1) {
				double rainAmount = Double.parseDouble(line.substring(
						index + 8, index + 12));
				if (rainAmount < 9998.5) {
					int rainInterval = Integer.parseInt(line.substring(
							index + 6, index + 8));
					double rainAvg = rainAmount / rainInterval * 24;
					if (rainAvg == 39996.0)
						System.err.println(rainAmount + " " + rainInterval);

					outputKey.set(stationID + "\t" + year);
					outputValue.set(rainAvg + "\t" + date);
					context.write(outputKey, outputValue);
				}
			}
		}

	}

	public static class RainAnomaliesReducer1 extends
			Reducer<Text, Text, Text, Text> {

		private Text outputValue = new Text();

		public void reduce(Text inputKey, Iterable<Text> inputValues,
				Context context) throws IOException, InterruptedException {
			double max = -1;
			String maxDate = "";
			for (Text txt : inputValues) {
				String[] split = txt.toString().split("\t");
				double val = Double.parseDouble(split[0]);
				if (val > max) {
					max = val;
					maxDate = split[1];
				}
			}
			outputValue.set(maxDate + "\t" + max);
			context.write(inputKey, outputValue);

		}
	}
}
