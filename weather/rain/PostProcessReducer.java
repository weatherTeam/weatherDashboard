package weather.rain;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PostProcessReducer extends Reducer<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void reduce(Text inputKey, Iterable<Text> inputValues,
			Context context) throws IOException, InterruptedException {
		String stationID = inputKey.toString().trim();
		if (!stationID.substring(0, 2).equals("US"))
			return;
		String latitude = "";
		String longitude = "";
		String state = "";
		String name = "";
		ArrayList<String> anomalies = new ArrayList<String>();
		for (Text txt : inputValues) {
			String line = txt.toString();
			if (line.startsWith("StationID")) {
				anomalies.add(line.substring(22).trim());
			} else {
				latitude = line.substring(13, 21).trim();
				longitude = line.substring(22, 31).trim();
				state = line.substring(39, 41).trim();
				name = line.substring(42, 72).trim();
			}
		}
		if (anomalies.size() == 0)
			return;
		outputKey
				.set("StationID:\t" + stationID + "\tLatitude:" + latitude
						+ "\tLongitude:" + longitude + "\tName:" + name + "\tState:"
						+ state);

		for (String anomaly : anomalies) {
			String[] tokens = anomaly.split("\t");
			String date = tokens[0].split("-")[1];
			String deviation = tokens[1].split("-")[1];
			String value = tokens[2].split("-")[1];
			String average = tokens[3].split("-")[1];
			outputValue.set("Date:\t" + date + "\tDeviation:" + deviation
					+ "\tValue:" + value + "\tAvg:" + average);

			context.write(outputKey, outputValue);
		}

	}
}