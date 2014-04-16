package weather.nearest_neighbor.advanced;

import java.io.*;
//import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class AdvancedNearestNeighbor {
    
    public static void main(String[] args) throws Exception {
    	
    	  // All months averages
          JobConf averageMonth = new JobConf(AdvancedNearestNeighbor.class);
          averageMonth.setJobName("NN advanced");

          averageMonth.setOutputKeyClass(Text.class);
          averageMonth.setOutputValueClass(IntWritable.class);
          
          averageMonth.setMapperClass(AverageMonthMapper.class);
          averageMonth.setReducerClass(AverageMonthReducer.class);

          FileInputFormat.setInputPaths(averageMonth, new Path(args[0]));
          FileOutputFormat.setOutputPath(averageMonth, new Path("/tmp/averageMonth"));
          
          averageMonth.setNumMapTasks(2);
          averageMonth.setNumReduceTasks(2);
          
          // Get averages for reference year (filter on one year)
          JobConf averageMonthYear = new JobConf(AdvancedNearestNeighbor.class);
          averageMonthYear.setJobName("NN advanced");

          averageMonthYear.setOutputKeyClass(Text.class);
          averageMonthYear.setOutputValueClass(IntWritable.class);
          
          averageMonthYear.set("referenceYear",args[2]);
          
          averageMonthYear.setMapperClass(AverageMonthYearMapper.class);

          FileInputFormat.setInputPaths(averageMonthYear, new Path("/tmp/averageMonth"));
          FileOutputFormat.setOutputPath(averageMonthYear, new Path("/tmp/averageMonthYear"));
          
          averageMonthYear.setNumMapTasks(2);
          

          // Calculate distances job configuration
          JobConf calculateDistances = new JobConf(AdvancedNearestNeighbor.class);
          calculateDistances.setJobName("Calculate Distances");
          
          calculateDistances.setOutputKeyClass(IntWritable.class);
          calculateDistances.setOutputValueClass(DoubleWritable.class);
          
          calculateDistances.setMapOutputKeyClass(IntWritable.class);
          calculateDistances.setMapOutputValueClass(Text.class);
          
          calculateDistances.setMapperClass(CalculateDistanceMapper.class);
          calculateDistances.setReducerClass(CalculateDistanceReducer.class);
          
          // Set filepath of referenceYearValues
          calculateDistances.set("path_to_file","/tmp/averageMonthYear/part-00000");

          FileInputFormat.setInputPaths(calculateDistances, new Path("/tmp/averageMonth"));
          FileOutputFormat.setOutputPath(calculateDistances, new Path(args[1]));
          
          calculateDistances.setNumMapTasks(2);
          calculateDistances.setNumReduceTasks(2);
  
          
          // Run jobs
          JobClient.runJob(averageMonth);
          JobClient.runJob(averageMonthYear);
          JobClient.runJob(calculateDistances);
          
    }
}

