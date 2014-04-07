package weather.snow.stationsStatistics;

import java.io.*;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * Created by Jo on 01.04.14.
 */
/*
* INPUT: raw data form NOAA
* OUTPUT: each station that contains information with the given identifier + number of records of this information
* for each station
 */
public class FindSnowStations {


	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
		                Reporter reporter) throws IOException {


			String input = value.toString();

			//for generalization, replace AJ1 by the desired identifier
			//AJ1: identifier for snow depth (if any)
			if (input.contains("AJ1")) {
				String stationID = input.substring(4, 10);
				output.collect(new Text(stationID), new IntWritable(1));
			}

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {


		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output,
		                   Reporter reporter) throws IOException {

            /*
             * Each key-value pair is of kind: "year-month" - {43, 50, 60, 30, 20, 0}. Values are all measured depth
             * in the time period given by the key
             */

			int nb = 0;

			while (values.hasNext()) {
				values.next();
				nb++;
			}

			output.collect(key, new Text(nb + ""));


		}


	}


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		//Path p = new Path(args[1]);
		Path tempPath = new Path(args[1]);

		JobConf conf1 = new JobConf(Run.class);
		conf1.setJobName("find snow stations");

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(IntWritable.class);

		conf1.setMapperClass(Map.class);
		conf1.setReducerClass(Reduce.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		conf1.setNumMapTasks(3);
		conf1.setNumReduceTasks(3);

		System.out.println("arg0" + args[0]);

		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf1, tempPath);


		JobClient.runJob(conf1);
	}


}
