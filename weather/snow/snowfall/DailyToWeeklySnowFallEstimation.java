package weather.snow.snowfall;

/**
 * Created by Jonathan Duss on 26.04.14.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;


/**
 * Created by Jonathan Duss on 01.04.14.
 * INPUT REQUIRED: output of dailySnowFallEstimation
 *
 *
 */
public class DailyToWeeklySnowFallEstimation {


	/**
	 * Preprocess the data: if snow depth is given, it may be measured each XX hours.
	 * <p/>
	 * output:
	 * key: stationID_year/month/day
	 * value: temperature  snowDepth snow_cum_from_depth rain snowDepth snow_cum_from_rain
	 */
	public static class MapperDailySnowComputation extends MapReduceBase implements Mapper<LongWritable, Text, Text,
			Text> {


		Text outKey = new Text();
		Text outVal = new Text();


		//input: 725315	2000/01/24	3
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
		                Reporter reporter) throws IOException {


			String input = value.toString();


			String split[] = input.split("\\s+");

			String stationID = split[0];
			String latitude = split[1];
			String longitude = split[2];
			String dateString = split[3];
			String cumulationString = split[4];


			//read and format DATE
			Calendar c = Calendar.getInstance();

			SimpleDateFormat dateFormatterReader = new SimpleDateFormat("yyyy/MM/dd");
			Date date = dateFormatterReader.parse(dateString, new ParsePosition(0));
			c.setTime(date);

			//Reformat the date to Year/Week
			SimpleDateFormat dateFormatterForKey = new SimpleDateFormat("yyyy-ww"); //"year/week in year"

			String time = dateFormatterForKey.format(c.getTime());

			outKey = new Text(stationID + "\t" + latitude + "\t" + longitude + "\t" + time);
			outVal = new Text(cumulationString);

			output.collect(outKey, outVal);


		}


	}


	/*
	*
	* OUTPUT KEY: STATION ID + tab + LATITUDE + tab + LONGITUDE + tab + TIME
	* OUTPUT VALUE: snow cumulation of each week
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
		                   Reporter reporter) throws IOException {

            /*
             * Each key-value pair is of kind: "year-week" - {43, 50, 60, 30, 20, 0}. Values are all measured depth
             * in the time period given by the key
             */

			try {
				Text outKey = key;
				Text outVal = new Text();
				int cumulationInWeek = 0;


				while (values.hasNext()) {
					cumulationInWeek += Integer.parseInt(values.next().toString());
				}

				if(cumulationInWeek > 0) {
					outVal = new Text("" + cumulationInWeek);
					output.collect(outKey, outVal);
				}

			} catch (NumberFormatException e) {
				System.out.println("a number from DailySnowFallEstimation wasn't read as a number in " +
						"DailyToWeeklySnowFallEstimation");
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
		conf1.setJobName("compute weekly snow computation");

		conf1.setMapperClass(MapperDailySnowComputation.class);
		conf1.setReducerClass(Reduce.class);

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



