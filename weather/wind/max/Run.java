package weather.wind.max;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Run
{	
	public static void main(String[] args) throws Exception
	{	
		// JOB 1
		JobConf conf1 = new JobConf(Run.class);
		conf1.setJobName("Wind");
		conf1.setNumMapTasks(40);
		conf1.setNumReduceTasks(40);
		
//		conf1.set("WIND_TRESHOLD", args[2]);
//		conf1.set("RAIN_TRESHOLD", args[3]);
		
		conf1.setMapOutputKeyClass(IntWritable.class);
		conf1.setMapOutputValueClass(Text.class);
		
		conf1.setOutputKeyClass(IntWritable.class);
		conf1.setOutputValueClass(Text.class);
 	
		conf1.setMapperClass(FindXtremMapper1.class);
		conf1.setReducerClass(FindXtremReducer1.class);
 	
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);
 	
		// JOB 2
		JobConf conf2 = new JobConf(Run.class);
		conf2.setJobName("Find Date Event");
		conf2.setNumMapTasks(40);
		conf2.setNumReduceTasks(40);
		
		//conf1.set("WIND_TRESHOLD", args[2]);
		
		conf2.setMapOutputKeyClass(IntWritable.class);
		conf2.setMapOutputValueClass(Text.class);
		
		conf2.setOutputKeyClass(IntWritable.class);
		conf2.setOutputValueClass(Text.class);
 	
		conf2.setMapperClass(FindDateEventMapper2.class);
		conf2.setReducerClass(FindDateEventReducer2.class);
 	
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		
		// JOB 3
		JobConf conf3 = new JobConf(Run.class);
		conf3.setJobName("Clustering event");
		conf3.setNumMapTasks(40);
		conf3.setNumReduceTasks(40);
		
		//conf1.set("WIND_TRESHOLD", args[2]);
		
		conf3.setMapOutputKeyClass(IntWritable.class);
		conf3.setMapOutputValueClass(Text.class);
		
		conf3.setOutputKeyClass(IntWritable.class);
		conf3.setOutputValueClass(Text.class);
 	
		conf3.setMapperClass(ClusterMapper3.class);
		conf3.setReducerClass(ClusterReducer3.class);
 	
		conf3.setInputFormat(TextInputFormat.class);
		conf3.setOutputFormat(TextOutputFormat.class);
		
		// JOB 4
		JobConf conf4 = new JobConf(Run.class);
		conf4.setJobName("Find Center");
		conf4.setNumMapTasks(40);
		conf4.setNumReduceTasks(1);
		
		conf4.setMapOutputKeyClass(Text.class);
		conf4.setMapOutputValueClass(Text.class);
		
		conf4.setOutputKeyClass(Text.class);
		conf4.setOutputValueClass(Text.class);
 	
		conf4.setMapperClass(FindCenterMapper4.class);
		conf4.setReducerClass(FindCenterReducer4.class);
 	
		conf4.setInputFormat(TextInputFormat.class);
		conf4.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		
		
		// Paths for the server
		FileOutputFormat.setOutputPath(conf1, new Path("/team11/tmp/CedricAlexis/1tmpXtrem"));

		FileInputFormat.setInputPaths(conf2, new Path("/team11/tmp/CedricAlexis/1tmpXtrem"));
		FileOutputFormat.setOutputPath(conf2, new Path("/team11/tmp/CedricAlexis/2tmpDateCluster"));

		FileInputFormat.setInputPaths(conf3, new Path("/team11/tmp/CedricAlexis/2tmpDateCluster"));
		FileOutputFormat.setOutputPath(conf3, new Path("/team11/tmp/CedricAlexis/3tmpCluster"));
		
		FileInputFormat.setInputPaths(conf4, new Path("/team11/tmp/CedricAlexis/3tmpCluster"));
		

		// Paths for local run
		FileOutputFormat.setOutputPath(conf1, new Path("output/1tmpXtrem"));

		FileInputFormat.setInputPaths(conf2, new Path("output/1tmpXtrem"));
		FileOutputFormat.setOutputPath(conf2, new Path("output/2tmpDateCluster"));

		FileInputFormat.setInputPaths(conf3, new Path("output/2tmpDateCluster"));
		FileOutputFormat.setOutputPath(conf3, new Path("output/3tmpCluster"));
		
		FileInputFormat.setInputPaths(conf4, new Path("output/3tmpCluster"));

		
		FileOutputFormat.setOutputPath(conf4, new Path(args[1]));

		JobClient.runJob(conf1);
		JobClient.runJob(conf2);
		JobClient.runJob(conf3);
		JobClient.runJob(conf4);

	}
}
