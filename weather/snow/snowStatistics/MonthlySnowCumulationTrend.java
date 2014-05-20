package weather.snow.snowStatistics;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import weather.snow.snowfall.DailySnowFallEstimation;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Jonathan Duss on 07.05.14.
 *
 * INPUT: Monthly snow cumulation
 * OUTPUT: Year-Month TAB Lattitude TAB longitude TAB monthly snow cumulation
 *
 * Try to find trend about the snow cumulation over 30 years.
 */
public class MonthlySnowCumulationTrend {
	/**
	 *
	 * <p/>
	 * output:
	 * key: stationID _year/month/day
	 * value: temperature  snowDepth snow_cum_from_depth rain snowDepth snow_cum_from_rain
	 */
	public static class GroupeByStationByMonth extends MapReduceBase implements Mapper<LongWritable, Text, Text,
			Text> {


		Text outKey = new Text();
		Text outVal = new Text();


		//input: 725315	2000/01/24	3
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
		                Reporter reporter) throws IOException {


			String input = value.toString();


			String split[] = input.split("\\s+");
			String dateString = split[3];
			String cumulationString = split[4];

			try{
				//if(Integer.parseInt(cumulationString) > 0){

					outKey = new Text(dateString);
					outVal = new Text(cumulationString);

					output.collect(outKey, outVal);
				//}
			} catch(NumberFormatException e){

			}


		}


	}

	/*
	* Compute the snow cumulation over a month for each station
	 */
	public static class MonthlySnowCumulation extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		Text outKey = new Text();
		Text outVal = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
		                   Reporter reporter) throws IOException {

            /*
             * Each key-value pair is of kind: "year-month" - {43, 50, 60, 30, 20, 0}. Values are all measured depth
             * in the time period given by the key
             */

			try {

				int cumulationInMonth = 0;
				float numberStationHavingSnow = 0;


				while (values.hasNext()) {
					int cumulation = Integer.parseInt(values.next().toString());
						cumulationInMonth += cumulation;
						numberStationHavingSnow ++;
				}



				outKey = key;
				outVal = new Text("" + Math.round(cumulationInMonth / numberStationHavingSnow));
				output.collect(outKey, outVal);

			} catch (NumberFormatException e) {
				System.out.println("a number from DailySnowFallEstimation wasn't read as a number in " +
						"DailyToMonthlySnowFallEstimation");
			}


		}


	}


	/**
	 * @param args inputPath tempPath outputPath
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub


		Path outPath = new Path(args[1]);
		Path inPath = new Path(args[0]);


		JobConf conf1 = new JobConf(DailySnowFallEstimation.class);
		conf1.setJobName("Monthly snow cumulation tred");

		conf1.setMapperClass(GroupeByStationByMonth.class);
		conf1.setReducerClass(MonthlySnowCumulation.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		conf1.setNumMapTasks(80);
		conf1.setNumReduceTasks(80);

		conf1.setMapOutputKeyClass(Text.class);
		conf1.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf1, inPath);
		FileOutputFormat.setOutputPath(conf1, outPath);


		JobClient.runJob(conf1);


	}


}

