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
		int day;
    	int[] referenceYearStationValues;

		while((line = bufferReader.readLine()) != null){
			
			station_id = line.substring(0,6);
			day = Integer.parseInt(line.substring(12,14));
			
			if (!referenceYearValues.containsKey(station_id)){
				referenceYearValues.put(station_id, new int[31]);
			}
			
			referenceYearStationValues = referenceYearValues.get(station_id); 
	  	  	referenceYearStationValues[day-1] = Integer.parseInt(line.split("\\s+")[1]);

	    }
	    bufferReader.close();
	    return referenceYearValues;

	}
}
