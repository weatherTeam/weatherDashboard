package weather.nearest_neighbor.advanced;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class utils {
    // Read in reference year average values
	static public int[] readInReferenceYear(String path_to_file) throws IOException{

		Path pt = new Path("hdfs://localhost:54310"+path_to_file);
		FileSystem fs = FileSystem.get(new Configuration());
		
		BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
	    
		String line;
	    
		int[] referenceYearValues = new int[12];
	    int month = 1;
	    while((line = bufferReader.readLine()) != null){
	  	  referenceYearValues[month-1] = Integer.parseInt(line.split("\\s+")[1]);
	  	  month++;
	    }
	    bufferReader.close();
	    return referenceYearValues;

	}
}
