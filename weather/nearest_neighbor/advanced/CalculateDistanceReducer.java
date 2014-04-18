package weather.nearest_neighbor.advanced;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CalculateDistanceReducer extends MapReduceBase implements 
	Reducer<Text, Text, Text, DoubleWritable> {   
    
	private String station;
	private String year;
    private double temp;
    private int month;
    private String[] textArray;
    
    private double distance;
    private int[] referenceYearStationValues= new int[12];
    
    private String path_to_file;
    
    Map<String, int[]> referenceYearValues = new HashMap<String, int[]>();

	public void configure(JobConf job) {
		path_to_file = job.get("path_to_file");
	}
	    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
                 throws IOException {
    		
    		station = key.toString().substring(0,6);
    		year = key.toString().substring(6,10);
            distance = 0.0;          
    		//System.out.println(Arrays.toString(referenceYearValues));
            referenceYearValues = utils.readInReferenceYear(path_to_file);
            referenceYearStationValues = referenceYearValues.get(station);

            while (values.hasNext()){
                textArray = values.next().toString().split(";");
                month = Integer.parseInt(textArray[0]);
                temp = Double.parseDouble(textArray[1]);
                
                distance += Math.pow(referenceYearStationValues[month-1]-temp, 2);
            }
            distance = Math.sqrt(distance);
            output.collect(new Text(year), new DoubleWritable(distance));
        }
    }
