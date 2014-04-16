package weather.nearest_neighbor.advanced;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CalculateDistanceReducer extends MapReduceBase implements 
	Reducer<IntWritable, Text, IntWritable, DoubleWritable> {   
    
	private int[] referenceYearValues;
    private double temp;
    private int month;
    private String[] textArray;
    
    private double distance;
    
    private String path_to_file;
    
	public void configure(JobConf job) {
		path_to_file = job.get("path_to_file");
	}
    
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
                 throws IOException {
        
            distance = 0.0;
            referenceYearValues = utils.readInReferenceYear(path_to_file);
    		System.out.println(Arrays.toString(referenceYearValues));

            while (values.hasNext()){
                textArray = values.next().toString().split(";");
                month = Integer.parseInt(textArray[0]);
                temp = Double.parseDouble(textArray[1]);
                
                distance += Math.pow(referenceYearValues[month-1]-temp, 2);
            }
            distance = Math.sqrt(distance);
            output.collect(key, new DoubleWritable(distance));
        }
    }
