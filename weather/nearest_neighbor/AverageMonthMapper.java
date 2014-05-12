package weather.nearest_neighbor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// This mapper emits values for each station
// Corresponding reducer calculates the averages for each station

public class AverageMonthMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
	
	private static final String MISSING = "9999";
	private static String referenceMonth;
	
	public void configure(JobConf job) {
		referenceMonth = job.get("referenceMonth");
	}
	
	public void map(LongWritable key, Text value,
		OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
	
        String line = value.toString();
        
        String year = line.substring(15, 19);
        int intYear = Integer.parseInt(year);
        
        String station = line.substring(4, 10);
        String month = line.substring(19, 21);
        
        // Divide month into periods to compare to each other. 
        // Can also compare day to day averages
        int day = Integer.parseInt(line.substring(21,23));
        //int period = (day-1)/5;
        //if (day==31){
        //	period = 5;
        //}
        
        // For USA, the region is to big to divide month into periods, thus we
        // use the average for the whole month, which means only one period
        int period = 0;
        
        String airTemperature;
        String precipitation = MISSING; 

        // Get air temperature
        if (line.charAt(87) == '+') {
            airTemperature = line.substring(88, 92);
        } else {
            airTemperature = line.substring(87, 92);
        }

        // Get precipitation    
        int index = line.indexOf("ADDAA1");
        if (index != -1){
            precipitation = line.substring(index+8,index+12);
        }

        // Emit station, date, temp and precipitation    
        String quality = line.substring(92, 93);
        if (!airTemperature.equals(MISSING) && quality.matches("[01459]") 
                && referenceMonth.equals(month)) {
            output.collect(new Text(station+intYear+month+period), 
                    new Text(airTemperature+";"+precipitation));
        }
	}
}
