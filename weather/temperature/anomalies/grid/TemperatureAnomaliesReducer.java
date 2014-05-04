package weather.temperature.anomalies.grid;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TemperatureAnomaliesReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		int temperature = 0;
		int temperatureMax = 0;
		int temperatureMin = 0;
		int averageTemperature = 0;
		int averageMax = 0;
		int averageMin = 0;
		boolean refExists = false;
		boolean valExists = false;
		while (values.hasNext()) {
			
			String[] val = values.next().toString().split(",");
			System.out.println(val);
			if(val[0].equals("$")) {
				refExists = true;
				averageTemperature = Integer.parseInt(val[1]);
				averageMax = Integer.parseInt(val[2]);
				averageMin = Integer.parseInt(val[3]);
			} else {
				valExists = true;
				temperature = Integer.parseInt(val[1]);
				temperatureMax = Integer.parseInt(val[2]);
				temperatureMin = Integer.parseInt(val[3]);
			}
		}
		if (!refExists || !valExists)
			return;
		int temperatureAnomaly = temperature-averageTemperature;
		int maxAnomaly = temperatureMax-averageMax;
		int minAnomaly = temperatureMin-averageMin;
		String keystr = key.toString();
		String lat = keystr.substring(0,6);
		String lon = keystr.substring(6,13);
		String year = keystr.substring(13, 17);
		String month = keystr.substring(17, 19);
		
		boolean isExtreme = ((temperature > averageMax) || (temperature< averageMin));
		//output.collect(new Text(lat+"\t"+lon+"\t"+year+"\t"+month), new Text(temperatureAnomaly+""));
		output.collect(new Text(year), new Text(lat+","+lon+","+month+","+temperatureAnomaly+","+maxAnomaly+","+minAnomaly+","+(isExtreme?"EXTREME":"NORMAL")));
	}
}