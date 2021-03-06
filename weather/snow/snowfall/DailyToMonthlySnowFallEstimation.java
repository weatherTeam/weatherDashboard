package weather.snow.snowfall;

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
 * Created by Jonathan Duss on 07.05.14.
 *
 * INPUT: Daily snow cumulation
 * OUTPUT: monthly snow cumulation
 * OUTPUT format: YYYY-MM tab LATITUDE tab LONGITUDE tab snow cumulation
 */
public class DailyToMonthlySnowFallEstimation {
	/**
	 * Preprocess the data: if snow depth is given, it may be measured each XX hours.
	 * <p/>
	 * output:
	 * key: stationID TAB latitude TAB longitude TAB year-month
	 * value: snow cumulation of the month
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
			SimpleDateFormat dateFormatterForKey = new SimpleDateFormat("yyyy-MM"); //"year/week in year"

			String time = dateFormatterForKey.format(c.getTime());

			outKey = new Text(stationID + "\t" + latitude + "\t" + longitude + "\t" + time);
			outVal = new Text(cumulationString);

			output.collect(outKey, outVal);


		}


	}


	/*
	* Compute the snow cumulation over a month for each station
	* output key: stationID TAB latitude TAB longitude TAB year-month
	* output value: snow cumulation of the month
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


				while (values.hasNext()) {
					cumulationInMonth += Integer.parseInt(values.next().toString());
				}

				outKey = key;
				outVal = new Text("" + cumulationInMonth);
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
		conf1.setJobName("daily to monthly snow cumulation");

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

