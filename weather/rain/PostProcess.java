package weather.rain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PostProcess {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: CMD <data path> <station path> <output Path>");
			System.exit(-1);
		}

		Job job = PostProcess.getJobPostProcessing(args[0],args[1],args[2]);
		job.waitForCompletion(true);

		System.exit(0);

	}

	public static Job getJobPostProcessing(String dataPath, String stationPath, String outputPath) throws IOException {
		Job job = Job.getInstance();
		job.setJarByClass(PostProcess.class);
		job.setJobName("Postprocessing of output");

		job.setMapperClass(PostProcessMapper.class);
		job.setReducerClass(PostProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputPath), true);

		FileInputFormat.addInputPath(job, new Path(dataPath));
		FileInputFormat.addInputPath(job, new Path(stationPath));

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job;
	}


}
