package weather.temperature.anomalies.grid;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AverageMonthYearTemperatureReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		int temperature = 0;
		int nbRecords = 0;
		while (values.hasNext()) {
			temperature += values.next().get();
			nbRecords++;
		}
		temperature /= nbRecords;
		output.collect(key, new IntWritable(temperature));
	}
}