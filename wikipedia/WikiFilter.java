
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WikiFilter {
	
public static String[] keywords = {"blizzard","cyclone","hurricane", "typhoon" ,"derecho","drought","nor'easter","storm","tornado","wave", "weather event"};
	
public static class WFMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		}
	}


	public static class WFReduce extends MapReduceBase implements Reducer<Text, IntWritable, NullWritable, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<NullWritable, IntWritable> output, Reporter reporter) throws IOException {
			

		}

	}	
	
	public static class WFCombine extends MapReduceBase implements Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

		@Override
		public void reduce(NullWritable key, Iterator<IntWritable> values, OutputCollector<NullWritable, IntWritable> output, Reporter reporter) throws IOException {
			

		}

	}

	public static void main(String[] args) throws Exception {
		
		
		boolean local = true;
				
		JobConf conf = new JobConf(WikiFilter.class);
		conf.setJobName("Wikipedia filtering");
		conf.setNumMapTasks(88);
		conf.setNumReduceTasks(176);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setMapperClass(WFMap.class);
		conf.setReducerClass(WFReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		
		if(!local){
			FileOutputFormat.setOutputPath(conf, new Path("/std022/hw2/tmp"));
		} else {
			FileOutputFormat.setOutputPath(conf, new Path("output"));
		}

		JobClient.runJob(conf);
		
		
		/*
		// We delete the tmp folder
		FileSystem fs = FileSystem.get(new Configuration());
		if(local){
			fs.delete(new Path("tmp"), true);
		} else {
			fs.delete(new Path("/std022/hw2/tmp"), true);
		}
		*/

	}
}
