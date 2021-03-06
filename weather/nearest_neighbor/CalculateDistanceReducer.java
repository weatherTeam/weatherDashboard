package weather.nearest_neighbor;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/* This reducer reads in the values for the reference year
 * and calculate the euclidean distance to the other years
 * values which it recieves from the mapper
 */

public class CalculateDistanceReducer extends MapReduceBase implements 
	Reducer<Text, Text, Text, DoubleWritable> {   
    
	private String station;
	private String year;
	private String month;
    private double temp;
    private double prec;
    
    private int period;
    private String[] textArray;
    
    private double distance;
    private int[] referenceYearStationValues= new int[12];
    
    private String path_to_file;
    
    Map<String, int[]> referenceYearValues = new HashMap<String, int[]>();

	public void configure(JobConf job) {
		path_to_file = job.get("path_to_file");
	}
	    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, 
    		Reporter reporter) throws IOException {
    		
    		String keyString = key.toString();
    		station = keyString.substring(0,6);
    		year = keyString.substring(6,10);
    		month = keyString.substring(10,12);
            distance = 0.0;          
            referenceYearValues = utils.readInReferenceYear(path_to_file);
            referenceYearStationValues = referenceYearValues.get(station);
            if (referenceYearStationValues != null){
            	while (values.hasNext() ){
            		textArray = values.next().toString().split(":");
            		period = Integer.parseInt(textArray[0]);
            		temp = Double.parseDouble(textArray[1].split(";")[0]);
            		prec = Double.parseDouble(textArray[1].split(";")[1]);
            	
                    // Only calculate distance for precipitation if it has values for
                    // both years    
            		if (prec >= 9999 || referenceYearStationValues[period*2+1]>= 9999){
            			distance += Math.abs(referenceYearStationValues[period*2]-temp);
            		} else {
            			distance += Math.sqrt(Math.pow(referenceYearStationValues[period*2]-temp,2) + 
            				Math.pow(referenceYearStationValues[period*2+1]-prec,2));
            		}
            	}
            	output.collect(new Text(year+month), new DoubleWritable(distance));
            }
        }
    }
