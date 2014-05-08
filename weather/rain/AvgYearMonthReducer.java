package weather.rain;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgYearMonthReducer extends Reducer<Text, IntWritable, Text, Text> {

	private Text outputValue = new Text();

	public void reduce(Text inputKey, Iterable<IntWritable> inputValues,
			Context context) throws IOException, InterruptedException {
		ArrayList<Integer> list = new ArrayList<Integer>();

		double avgSum = 0.0;

		for (IntWritable val : inputValues) {
			int intVal = val.get();
			avgSum += intVal;
			list.add(intVal);
		}
		String granul = context.getConfiguration().get("granul", "day");
		if(granul.equals("month") && list.size()<25)
			return;
		avgSum /= list.size();

		DecimalFormat df = new DecimalFormat("#.###");

		outputValue.set(df.format(avgSum));
		context.write(inputKey, outputValue);

	}
}
