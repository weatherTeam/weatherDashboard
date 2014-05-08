package weather.rain;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnomaliesDetectionReducer extends Reducer<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();

	public void reduce(Text inputKey, Iterable<Text> inputValues,
			Context context) throws IOException, InterruptedException {

		ArrayList<String> list = new ArrayList<String>();
		String granul = context.getConfiguration().get("granul", "day");

		String ID = inputKey.toString().split(",")[0];
		String month = inputKey.toString().split(",")[1];
		String day = "";
		if (granul.equals("day"))
			day = inputKey.toString().split(",")[2];
		int beginyear = context.getConfiguration().getInt("beginyear", 1900);
		int endyear = context.getConfiguration().getInt("endyear", 2020);
		int difference = endyear - beginyear;

		double average = 0, stddev = 0;

		for (Text val : inputValues) {
			String rains = val.toString();
			String[] stats = rains.split(",");
			try {
				average += Double.parseDouble(stats[0]);
				list.add(rains);
			} catch (NumberFormatException e) {
			}
		}
		if (list.size() * 2 <= difference)
			return;
		average /= list.size();

		for (String total : list) {
			try {
				double val = Double.parseDouble(total.split(",")[0]);
				stddev += (average - val) * (average - val);
			} catch (NumberFormatException e) {
			}
		}
		stddev = Math.sqrt(stddev / list.size());

		if (average != 0 && stddev != 0) {
			for (String total : list) {
				try {
					double val = Double.parseDouble(total.split(",")[0]);
					String year = total.split(",")[1];
					double deviation = Math.abs(val - average) / stddev;
					double limit = Double.parseDouble(context
							.getConfiguration().get("deviation"));
					if (deviation >= limit) {
						DecimalFormat df = new DecimalFormat("#.###");

						if (granul.equals("day"))
							outputKey.set("StationID-" + ID + "\tDate-" + year
									+ "." + month + "." + day);
						else if (granul.equals("month"))
							outputKey.set("StationID-" + ID + "\tDate-" + year
									+ "." + month);

						outputValue.set("Deviation-" + df.format(deviation)
								+ "\tValue-" + val + "\tAverage-"
								+ df.format(average));
						context.write(outputKey, outputValue);
					}
				} catch (NumberFormatException e) {
				}
			}
		}

	}
}