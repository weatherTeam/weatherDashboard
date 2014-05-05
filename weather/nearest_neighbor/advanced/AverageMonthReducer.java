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
	Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
	
		int temperature = 0;
		int tempNumRecords = 0;
		
		int precipitation = 0;
		int precNumRecords = 0;
		boolean hasValues = false;
		
		String[] value;
		while (values.hasNext()) {
			value = values.next().toString().split(";");
			
			temperature += Integer.parseInt(value[0]);
			tempNumRecords++;
			
			if (!value[1].equals("9999")){
				precipitation += Integer.parseInt(value[1]);
				hasValues = true;
				precNumRecords++;
			}
			
		}
		temperature /= tempNumRecords;
		if (hasValues){
			precipitation /= precNumRecords;
		} else {
			precipitation = 9999;
			}
		output.collect(key, new Text(temperature+";"+precipitation));
	}
}
