package weather.nearest_neighbor;

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
		
            // Sum up all temperatures    
			temperature += Integer.parseInt(value[0]);
            // Count records
			tempNumRecords++;
		
            // Sum precipitation (if it is not missing)
			if (!value[1].equals("9999")){
				precipitation += Integer.parseInt(value[1]);
				hasValues = true;
				precNumRecords++;
			}
			
		}
        // Calculate average
		temperature /= tempNumRecords;
        
        // Only calculate average if it has some records
		if (hasValues){
			precipitation /= precNumRecords;
		} else {
			precipitation = 9999;
			}
        // Emit (station+date, temp average + precipitation average)
		output.collect(key, new Text(temperature+";"+precipitation));
	}
}
