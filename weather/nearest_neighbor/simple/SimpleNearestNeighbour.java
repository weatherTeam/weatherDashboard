package weather.nearest_neighbor.simple;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class SimpleNearestNeighbour {
    
    public static class NNMapper1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        private double temp;
        private String date;
        
        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            
            String[] lineArray = value.toString().split(";");

            if (!lineArray[2].equals("NA") ){
                temp = Double.parseDouble(lineArray[2]);
                date = lineArray[1];
                output.collect(new Text(date), new DoubleWritable(temp));
            }
        }
    }
    
    public static class NNReducer1 extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {   

        private double mean;
        private int count;
        
        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
                     throws IOException {
                mean = 0;
                count = 0;
                while (values.hasNext()){
                    mean += values.next().get();
                    count++;
                }
                mean = mean/count;
                output.collect(key, new DoubleWritable(mean));
            }
        }
    
    public static class NNMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        
        private String temp;
        private String[] date;
        private int year;
        private String month;
        
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            
            String[] lineArray = value.toString().split("\\s+");

            date = lineArray[0].split("\\p{Punct}");

            year = Integer.parseInt(date[1]);

            month = date[0];
            
            temp = lineArray[1];
            
            output.collect(new IntWritable(year), new Text(month+";" +temp));
        }
    }
    
    public static class NNReducer2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, DoubleWritable> {   
        
        private double[] year2013 = {-10.683333333333332, -7.933333333333334, -7.883333333333334, -0.6666666666666666, 
                7.8166666666666655, 10.383333333333333, 13.700000000000001, 11.450000000000001, 7.716666666666666, 2.766666666666667,
                -2.066666666666667, -2.6142857142857143};
        private double temp;
        private int month;
        private String[] textArray;
        
        private double distance;
        
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
                     throws IOException {
            
                distance = 0.0;

                while (values.hasNext()){
                    System.out.println(values.next());
                    textArray = values.next().toString().split(";");
                    month = Integer.parseInt(textArray[0]);
                    temp = Double.parseDouble(textArray[1]);
                    
                    distance += Math.pow(year2013[month-1]-temp, 2);
                }
            
                output.collect(key, new DoubleWritable(distance));
            }
        }
    
    public static void main(String[] args) throws Exception {
          JobConf conf1 = new JobConf(SimpleNearestNeighbour.class);
          conf1.setJobName("NN simple phase 1");

          
          conf1.setOutputKeyClass(Text.class);
          conf1.setOutputValueClass(DoubleWritable.class);

          conf1.setMapperClass(NNMapper1.class);
          conf1.setReducerClass(NNReducer1.class);

          conf1.setInputFormat(TextInputFormat.class);
          conf1.setOutputFormat(TextOutputFormat.class);

          FileInputFormat.setInputPaths(conf1, new Path(args[0]));
          FileOutputFormat.setOutputPath(conf1, new Path("/tmp/hadoop"));
          
          conf1.setNumMapTasks(1);
          conf1.setNumReduceTasks(1);
          
          JobConf conf2 = new JobConf(SimpleNearestNeighbour.class);
          conf2.setJobName("NN simple phase 2");

          //conf2.setMapOutputKeyClass(IntWritable.class);
          //conf2.setMapOutputKeyClass(Text.class);
          
          conf2.setOutputKeyClass(IntWritable.class);
          conf2.setOutputValueClass(Text.class);

          conf2.setMapperClass(NNMapper2.class);
          conf2.setReducerClass(NNReducer2.class);
          
          conf2.setInputFormat(TextInputFormat.class);
          conf2.setOutputFormat(TextOutputFormat.class);

          FileInputFormat.setInputPaths(conf2, new Path("/tmp/hadoop/part-00000"));
          FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
          
          conf2.setNumMapTasks(1);
          conf2.setNumReduceTasks(1);
          
          // Run jobs
          JobClient.runJob(conf1);
          JobClient.runJob(conf2);
          
    }

}

