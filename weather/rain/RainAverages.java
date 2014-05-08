package weather.rain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RainAverages {


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 5) {
			System.err.println("Usage: CMD <input path> <output Path> <begin year> <end year> <precision>");
			System.exit(-1);
		}
		int begin = Integer.parseInt(args[2]);
		int end = Integer.parseInt(args[3]);

		Job job2 = RainAnomalies.getJobAvgYearMonth(args[0], args[1]+"-tmp");
		job2.getConfiguration().setInt("beginyear", begin);
		job2.getConfiguration().setInt("endyear", end);
		job2.setNumReduceTasks(1);
		job2.waitForCompletion(true);

		if(args[4].equals("month")){
			Job job3 = RainAverages.getJobMonthAverage(args[1]+"-tmp", args[1]);
			job3.setNumReduceTasks(1);
			job3.waitForCompletion(true);
		} else if(args[4].equals("year")){
			Job job4 = RainAverages.getJobYearAverage(args[1]+"-tmp", args[1]);
			job4.setNumReduceTasks(1);
			job4.waitForCompletion(true);
		}

		System.exit(0);

	}

	private static Job getJobYearAverage(String inputPath, String outputPath) throws IllegalArgumentException, IOException {
		Job job = Job.getInstance();
		job.setJarByClass(RainAverages.class);
		job.setJobName("Average of Yearly Rainfall");

		job.setMapperClass(YearAverageMapper.class);
		job.setReducerClass(YearAverageReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputPath), true);
		FileInputFormat.addInputPath(job, new Path(inputPath));

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job;
	}

	private static Job getJobMonthAverage(String inputPath, String outputPath) throws IllegalArgumentException, IOException {
		Job job = Job.getInstance();
		job.setJarByClass(RainAverages.class);
		job.setJobName("Average of Monthly Rainfall");

		job.setMapperClass(MonthAverageMapper.class);
		job.setReducerClass(MonthAverageReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputPath), true);
		FileInputFormat.addInputPath(job, new Path(inputPath));

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job;
	}

}
