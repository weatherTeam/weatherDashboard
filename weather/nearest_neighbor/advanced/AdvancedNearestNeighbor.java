package weather.nearest_neighbor.advanced;

import java.io.IOException;
import java.util.*;

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
          FileOutputFormat.setOutputPath(averageMonth, new Path("/tmp/hadoop"));
          
          averageMonth.setNumMapTasks(1);
          averageMonth.setNumReduceTasks(1);
          
          // Get averages for reference year
          JobConf averageMonthYear = new JobConf(AdvancedNearestNeighbor.class);
          averageMonthYear.setJobName("NN advanced");

          averageMonthYear.setOutputKeyClass(Text.class);
          averageMonthYear.setOutputValueClass(IntWritable.class);
          
          averageMonthYear.set("referenceYear",args[2]);
          
          averageMonthYear.setMapperClass(AverageMonthMapper.class);
          averageMonthYear.setReducerClass(AverageMonthReducer.class);

          FileInputFormat.setInputPaths(averageMonthYear, new Path("/tmp/hadoop"));
          FileOutputFormat.setOutputPath(averageMonthYear, new Path(args[1]));
          
          averageMonthYear.setNumMapTasks(1);
          averageMonthYear.setNumReduceTasks(1);
          
          // Run jobs
          JobClient.runJob(averageMonth);
          JobClient.runJob(averageMonth);
          
    }

}

