import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * Created by Jo on 01.04.14.
 */
public class SnowFallEstimation {


	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {


			String input = value.toString();

			//for position: if in document there is 13-15, in java, we write 12-15.
			//the first changes because in java the index begin at 0 and in document at 1.
			//the second changes because in java, substring(a,b) is: from a included to b excluded
			String stationID = input.substring(4,10);

			String tempString = input.substring(87,92);


			if(input.contains("AJ1")){
				//output existing snow data
				output.collect(new Text(stationID), new IntWritable(1));
			}
			else {
				//estimate snow
			}

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {



		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            /*
             * Each key-value pair is of kind: "year-month" - {43, 50, 60, 30, 20, 0}. Values are all measured depth in the time period given by the key
             */

			int nb = 0;

			while(values.hasNext())
			{
				values.next();
				nb++;
			}

			output.collect(key, new Text(nb + ""));


		}


	}



	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
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

		conf1.setNumMapTasks(2);
		conf1.setNumReduceTasks(2);

		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf1, tempPath);


		JobClient.runJob(conf1);
	}



}
