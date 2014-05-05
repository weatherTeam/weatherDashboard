package weather.nearest_neighbor.advanced;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

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
	    
		//int[] referenceYearValues = new int[12];

		String station_id;
		int period;
    	int[] referenceYearStationValues;
    	String[] values;

		while((line = bufferReader.readLine()) != null){
			
			station_id = line.substring(0,6);
			period = Integer.parseInt(line.substring(12,13));
			
			if (!referenceYearValues.containsKey(station_id)){
				referenceYearValues.put(station_id, new int[12]);
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
