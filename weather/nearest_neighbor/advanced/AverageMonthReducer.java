package weather.nearest_neighbor.advanced;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AverageMonthReducer extends MapReduceBase implements 
	Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
	
		int temperature = 0;
		int nbRecords = 0;
		int t;
		while (values.hasNext()) {
			t = values.next().get();
			System.out.println(t);
			temperature += t;
			nbRecords++;
		}
		temperature /= nbRecords;
		output.collect(key, new IntWritable(temperature));
	}
}
