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
		JobConf conf1 = new JobConf(Run.class);
		conf1.setJobName("Wind");
		
		conf1.setMapOutputKeyClass(IntWritable.class);
		conf1.setMapOutputValueClass(Text.class);
		
		conf1.setOutputKeyClass(IntWritable.class);
		conf1.setOutputValueClass(Text.class);
 	
		conf1.setMapperClass(FirstMapper.class);
		conf1.setReducerClass(FirstReducer.class);
 	
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);
 	
		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

		JobClient.runJob(conf1);

	}
}