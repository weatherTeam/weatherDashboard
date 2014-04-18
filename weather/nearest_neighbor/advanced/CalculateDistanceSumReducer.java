package weather.nearest_neighbor.advanced;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CalculateDistanceSumReducer extends MapReduceBase 
	implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		
		private int num_stations;
		private Double sum;
	
		public void reduce(IntWritable key, Iterator<DoubleWritable> values, 
				OutputCollector<IntWritable, DoubleWritable> output, 
					Reporter reporter) throws IOException {
			
			num_stations = 0;
			sum = 0.0;
			
			while(values.hasNext()){
				sum += Double.parseDouble(values.next().toString());
				num_stations++;
			}
			sum /= num_stations;
			output.collect(key, new DoubleWritable(sum));
		}
}