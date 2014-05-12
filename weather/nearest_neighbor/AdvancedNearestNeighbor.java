package weather.nearest_neighbor;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/*
 * This file contains the job configuration for Nearest Neighbor
 *
 * It consits of four jobs:
 *  - Calculate averages
 *  - Filter for reference year
 *  - Calculate distances
 *  - Sum up total distances
 *
 * Input paramters is:
 * 
 *      input output year month
 * 
 * where "year" is the reference year and "month" is the
 * period to be compared
 *
 * Input data is of the standard NOAA form (ish-format)
 * 
 * Output is:
 *     
 *     year distance
 * 
 */

public class AdvancedNearestNeighbor {
    
    public static void main(String[] args) throws Exception {
    	
    	  // Calculate averages for the specified month for all years
          JobConf averageMonth = new JobConf(AdvancedNearestNeighbor.class);
          averageMonth.setJobName("Average Month Values");

          averageMonth.setOutputKeyClass(Text.class);
          averageMonth.setOutputValueClass(Text.class);
          
          averageMonth.set("referenceMonth",args[3]);
          
          averageMonth.setMapperClass(AverageMonthMapper.class);
          averageMonth.setReducerClass(AverageMonthReducer.class);

          FileInputFormat.setInputPaths(averageMonth, new Path(args[0]));
          FileOutputFormat.setOutputPath(averageMonth, new Path("/tmp/averageMonth"));
         
          // To be set on server (depending on load and parameters) 
          averageMonth.setNumMapTasks(100);
          averageMonth.setNumReduceTasks(100);
          
          // Job to filter out data for the reference year
          JobConf averageMonthYear = new JobConf(AdvancedNearestNeighbor.class);
          averageMonthYear.setJobName("Filter for specific year");

          averageMonthYear.setOutputKeyClass(Text.class);
          averageMonthYear.setOutputValueClass(Text.class);
          
          averageMonthYear.set("referenceYear",args[2]);
          
          averageMonthYear.setMapperClass(AverageMonthYearMapper.class);

          FileInputFormat.setInputPaths(averageMonthYear, new Path("/tmp/averageMonth"));
          FileOutputFormat.setOutputPath(averageMonthYear, new Path("/tmp/averageMonthYear"));
          
          // To be set on server (depending on load and parameters) 
          averageMonthYear.setNumMapTasks(100);
          

          // Job to calculate distances
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
          FileOutputFormat.setOutputPath(calculateDistances, new Path("/tmp/calculateDistances"));
          
          // To be set on server (depending on load and parameters) 
          calculateDistances.setNumMapTasks(100);
          calculateDistances.setNumReduceTasks(100);
          
          
          // Sum up results for each year and station
          JobConf calculateDistancesSum = new JobConf(AdvancedNearestNeighbor.class);
          calculateDistancesSum.setJobName("Calculate Distances Sum");
          
          calculateDistancesSum.setOutputKeyClass(IntWritable.class);
          calculateDistancesSum.setOutputValueClass(DoubleWritable.class);
          
          calculateDistancesSum.setMapOutputKeyClass(IntWritable.class);
          calculateDistancesSum.setMapOutputValueClass(DoubleWritable.class);
          
          calculateDistancesSum.setMapperClass(CalculateDistanceSumMapper.class);
          calculateDistancesSum.setReducerClass(CalculateDistanceSumReducer.class);
          
          FileInputFormat.setInputPaths(calculateDistancesSum, new Path("/tmp/calculateDistances"));
          FileOutputFormat.setOutputPath(calculateDistancesSum, new Path(args[1]));
          
          // To be set on server (depending on load and parameters) 
          calculateDistancesSum.setNumMapTasks(100);
          calculateDistancesSum.setNumReduceTasks(1);
          
          // Run jobs
          JobClient.runJob(averageMonth);
          JobClient.runJob(averageMonthYear);
          JobClient.runJob(calculateDistances);
          JobClient.runJob(calculateDistancesSum);
    }
}
