package weather.rain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AnomaliesDetectionMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	@Override
	public void map(LongWritable inputKey, Text inputValue, Context context)
			throws IOException, InterruptedException {
		String line = inputValue.toString();
		String[] parts = line.split("\t");
		String value = parts[1];
		String[] parts2 = parts[0].split(",");
		String stationId = parts2[0];
		String month = parts2[1];
		String year = parts2[2];
		String granul = context.getConfiguration().get("granul", "day");
		if(granul.equals("day")){
			String day = parts2[3];
			outputKey.set(stationId + "," + month + "," + day); // ID + month + day
		} else if(granul.equals("month"))
			outputKey.set(stationId + "," + month); // ID + month
		outputValue.set(value + "," + year); // value + year
		context.write(outputKey, outputValue);

	}

}