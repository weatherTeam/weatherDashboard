package weather.nearest_neighbor.advanced;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class utils {
    // Read in reference year average values
	static public Map<String,int[]> readInReferenceYear(String path_to_file) throws IOException{

		//Path pt = new Path("hdfs://localhost:54310"+path_to_file);
		Path pt = new Path("/tmp/averageMonthYear/part-00000");
		FileSystem fs = FileSystem.get(new Configuration());
		
		BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
	    
		String line;
		Map<String,int[]> referenceYearValues = new HashMap<String, int[]>();
	    
		//int[] referenceYearValues = new int[12];

		String station_id = new String();
		
		int month = 1;
    	int[] referenceYearStationValues = new int[12];

		while((line = bufferReader.readLine()) != null){

	  	  	referenceYearStationValues[month-1] = Integer.parseInt(line.split("\\s+")[1]);
	  	  	month++;

			if (month == 13){
				station_id = line.substring(0,6);
				referenceYearValues.put(station_id, referenceYearStationValues);
				referenceYearStationValues = new int[12];
				month = 1;
			}
	    }
	    bufferReader.close();
	    return referenceYearValues;

	}
}
