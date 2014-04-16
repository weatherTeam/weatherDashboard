package weather.nearest_neighbor.advanced;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class utils {
    // Read in reference year average values
	static public int[] readInReferenceYear(String path_to_file) throws IOException{
	    FileReader inputFile = new FileReader(path_to_file);
	    BufferedReader bufferReader = new BufferedReader(inputFile);
	    String line;
	    int[] referenceYearValues = new int[12];
	    int month = 1;
	    while((line = bufferReader.readLine()) != null){
	  	  referenceYearValues[month-1] = Integer.parseInt(line.split("\\s+")[1]);
	  	  month++;
	    }
	    bufferReader.close();
	    inputFile.close();
	    return referenceYearValues;
	}
}
