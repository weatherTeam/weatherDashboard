package weather.snow.snowfall;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * Created by Jonathan Duss on 01.04.14.
 */
public class SnowFallEstimation {


	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {


			String input = value.toString();

			//for position: if in document there is 13-15, in java, we write 12-15.
			//the first changes because in java the index begin at 0 and in document at 1.
			//the second changes because in java, substring(a,b) is: from a included to b excluded
			String stationID = input.substring(4,10);

			String tempString = input.substring(87,92);



			//estimation


			if(input.contains("AA1")){
				//measure every hour
				//output existing snow data
				//utput.collect(new Text(stationID), new IntWritable(1));
				int startPosition = input.indexOf("AA101");
				int endPosition = startPosition+10;

				String precipitationData = input.substring(startPosition,endPosition);

				//Which weather? Can help find snow condition between -1 and 3 °C, where it can rain or snow (or  both)
				//See ish-format-document.pdf pages 26 to 32
				String precipitationTypePresentAutomated = ""; // TODO
				String precipitationTypePresentManual = ""; // TODO
				String precipitationTypePastManual = ""; // TODO
				String precipitationTypePastAutomated = ""; // TODO





				float precipitationAmount = Float.parseFloat(precipitationData.substring(5,9));
				int precipitationMeasureInterval = Integer.parseInt(precipitationData.substring(3, 5));

			}


			//no precipitation data

		}

		public float estimateSnowFall(float temperature, float hourlyPrecipitation, int precipitationType ){
			//will use or temperature, or precipitation type to see if it is snow.
			//then will compute the equivalent amount of snow based on the temperature

			return 0;
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {



		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            /*
             * Each key-value pair is of kind: "year-month" - {43, 50, 60, 30, 20, 0}. Values are all measured depth in the time period given by the key
             */

			int nb = 0;

			while(values.hasNext())
			{
				values.next();
				nb++;
			}

			output.collect(key, new Text(nb + ""));


		}


	}



	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

		//Path p = new Path(args[1]);
		Path tempPath = new Path(args[1]);

		JobConf conf1 = new JobConf(Run.class);
		conf1.setJobName("find snow stations");

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(IntWritable.class);

		conf1.setMapperClass(Map.class);
		conf1.setReducerClass(Reduce.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		conf1.setNumMapTasks(2);
		conf1.setNumReduceTasks(2);

		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf1, tempPath);


		JobClient.runJob(conf1);
	}



}
