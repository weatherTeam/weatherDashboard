package weather.nearest_neighbor;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/* This file contains helper functions
 * 
 * There is one important function that reads in the reference year values
 * It reads from the hdfs file system, so this path must be changed according
 * to which enviroment you are running the job on
 *
 * If the number of periods is changed, this must also be changed here
 */

public class utils {
    // Read in reference year average values
	static public Map<String,int[]> readInReferenceYear(String path_to_file) throws IOException{

		//Path pt = new Path("hdfs://localhost:54310"+path_to_file); // personal server
		Path pt = new Path("/tmp/averageMonthYear/part-00000"); // local
		//Path pt = new Path("hdfs://icdatasrv5-priv:9000"+path_to_file); // icdataserver
		FileSystem fs = FileSystem.get(new Configuration());
		
		BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
	    
		String line;
		Map<String,int[]> referenceYearValues = new HashMap<String, int[]>();

		String station_id;
		int period;
    	int[] referenceYearStationValues;
    	String[] values;

		while((line = bufferReader.readLine()) != null){
			
			station_id = line.substring(0,6);
			period = Integer.parseInt(line.substring(12,13));
			
			if (!referenceYearValues.containsKey(station_id)){
				referenceYearValues.put(station_id, new int[2]);
			}
			
			referenceYearStationValues = referenceYearValues.get(station_id);
			values = line.split("\\s+")[1].split(";");
	  	  	referenceYearStationValues[period*2] = Integer.parseInt(values[0]);
	  	  	referenceYearStationValues[period*2+1] = Integer.parseInt(values[1]);
	    }
	    bufferReader.close();
	    return referenceYearValues;

	}
}
