package weather.rain;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MonthAverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private DoubleWritable outputValue = new DoubleWritable();

	public void reduce(Text inputKey, Iterable<DoubleWritable> inputValues,
			Context context) throws IOException, InterruptedException {

		double avgSum = 0.0;
		int size = 0;

		for (DoubleWritable val : inputValues) {
			double doubleVal = val.get();
			avgSum += doubleVal;
			size++;
		}
		avgSum /= size;

		outputValue.set(avgSum);
		context.write(inputKey, outputValue);

	}
}
