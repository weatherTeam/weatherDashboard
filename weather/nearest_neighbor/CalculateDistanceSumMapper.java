package weather.nearest_neighbor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CalculateDistanceSumMapper extends MapReduceBase implements 
	Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
	
	
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
			throws IOException {
		
		String[] lineArray = value.toString().split("\\s+");
		int yearMonth = Integer.parseInt(lineArray[0]);
		double temp = Double.parseDouble(lineArray[1]);
		output.collect(new IntWritable(yearMonth), new DoubleWritable(temp));
	}

}
