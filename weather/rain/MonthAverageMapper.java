package weather.rain;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MonthAverageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text outputKey = new Text();
	private DoubleWritable outputValue = new DoubleWritable();

	@Override
	public void map(LongWritable inputKey, Text inputValue, Context context)
			throws IOException, InterruptedException {
		String line = inputValue.toString();
		String[] parts = line.split("\t");
		try{
			String[] parts2 = parts[0].split(",");
			@SuppressWarnings("unused")
			String stationId = parts2[0];
			String month = parts2[1];
			@SuppressWarnings("unused")
			String year = parts2[2];
			outputKey.set(month); // month
			outputValue.set(Double.parseDouble(parts[1])); // value
			context.write(outputKey, outputValue);
		} catch(NumberFormatException e){}

	}
}
