package weather.rain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PostProcessMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	@Override
	public void map(LongWritable inputKey, Text inputValue, Context context)
			throws IOException, InterruptedException {
		String line = inputValue.toString();
		if(line.startsWith("StationID")){ // from anomalies-file
			outputKey.set(line.substring(10,21));
			outputValue.set(line);
		} else{ // from stations-file
			outputKey.set(line.substring(0,11));
			outputValue.set(line);
		}

		context.write(outputKey, outputValue);

	}

}