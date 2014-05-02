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
		
		conf1.set("WIND_TRESHOLD", args[2]);
		conf1.set("RAIN_TRESHOLD", args[3]);
		
		conf1.setMapOutputKeyClass(IntWritable.class);
		conf1.setMapOutputValueClass(Text.class);
		
		conf1.setOutputKeyClass(IntWritable.class);
		conf1.setOutputValueClass(Text.class);
 	
		conf1.setMapperClass(FirstMapper.class);
		conf1.setReducerClass(FirstReducer.class);
 	
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);
 	
		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf1, new Path("/team11/tmp/CedricAlexis_250"));
//		FileOutputFormat.setOutputPath(conf1, new Path("tmp"));

		JobConf conf2 = new JobConf(Run.class);
		conf2.setJobName("Wind");
		
		//conf1.set("WIND_TRESHOLD", args[2]);
		
		conf2.setMapOutputKeyClass(IntWritable.class);
		conf2.setMapOutputValueClass(Text.class);
		
		conf2.setOutputKeyClass(IntWritable.class);
		conf2.setOutputValueClass(Text.class);
 	
		conf2.setMapperClass(SecondMapper.class);
		conf2.setReducerClass(SecondReducer.class);
 	
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
 	
		
		FileInputFormat.setInputPaths(conf2, new Path("/team11/tmp/CedricAlexis_250"));
//		FileInputFormat.setInputPaths(conf2, new Path("tmp"));
		FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

		JobClient.runJob(conf1);
		JobClient.runJob(conf2);

	}
}
