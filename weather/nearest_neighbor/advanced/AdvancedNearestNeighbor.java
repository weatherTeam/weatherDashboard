package weather.nearest_neighbor.advanced;

//import java.io.*;
//import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class AdvancedNearestNeighbor {
    
    public static void main(String[] args) throws Exception {
    	
    	  // All months averages
          JobConf averageMonth = new JobConf(AdvancedNearestNeighbor.class);
          averageMonth.setJobName("Average Month Values");

          averageMonth.setOutputKeyClass(Text.class);
          averageMonth.setOutputValueClass(IntWritable.class);
          
          averageMonth.setMapperClass(AverageMonthMapper.class);
          averageMonth.setReducerClass(AverageMonthReducer.class);

          FileInputFormat.setInputPaths(averageMonth, new Path(args[0]));
          FileOutputFormat.setOutputPath(averageMonth, new Path("/tmp/averageMonth"));
          
          averageMonth.setNumMapTasks(1);
          averageMonth.setNumReduceTasks(1);
          
          // Get averages for reference year (filter on one year)
          JobConf averageMonthYear = new JobConf(AdvancedNearestNeighbor.class);
          averageMonthYear.setJobName("Filter for specific year");

          averageMonthYear.setOutputKeyClass(Text.class);
          averageMonthYear.setOutputValueClass(IntWritable.class);
          
          averageMonthYear.set("referenceYear",args[2]);
          
          averageMonthYear.setMapperClass(AverageMonthYearMapper.class);

          FileInputFormat.setInputPaths(averageMonthYear, new Path("/tmp/averageMonth"));
          FileOutputFormat.setOutputPath(averageMonthYear, new Path("/tmp/averageMonthYear"));
          
          averageMonthYear.setNumMapTasks(1);
          

          // Calculate distances job configuration
          JobConf calculateDistances = new JobConf(AdvancedNearestNeighbor.class);
          calculateDistances.setJobName("Calculate Distances");
          
          calculateDistances.setOutputKeyClass(Text.class);
          calculateDistances.setOutputValueClass(DoubleWritable.class);
          
          calculateDistances.setMapOutputKeyClass(Text.class);
          calculateDistances.setMapOutputValueClass(Text.class);
          
          calculateDistances.setMapperClass(CalculateDistanceMapper.class);
          calculateDistances.setReducerClass(CalculateDistanceReducer.class);
          
          // Set filepath of referenceYearValues
          calculateDistances.set("path_to_file","/tmp/averageMonthYear/part-00000");

          FileInputFormat.setInputPaths(calculateDistances, new Path("/tmp/averageMonth"));
          FileOutputFormat.setOutputPath(calculateDistances, new Path(args[1]));
          
          calculateDistances.setNumMapTasks(1);
          calculateDistances.setNumReduceTasks(1);
  
          
          // Run jobs
          //JobClient.runJob(averageMonth);
          //JobClient.runJob(averageMonthYear);
          JobClient.runJob(calculateDistances);
          
    }
}
