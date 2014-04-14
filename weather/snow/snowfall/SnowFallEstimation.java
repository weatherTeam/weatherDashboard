package weather.snow.snowfall;

import java.io.*;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import javax.swing.text.DateFormatter;


/**
 * Created by Jonathan Duss on 01.04.14.
 */
public class SnowFallEstimation {


	/*
		Preprocess the data: if snow depth is given, it may be measured each XX hours.
	 */
	public static class MapPreprocessing extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		int lastDepth = -1;

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
		                Reporter reporter) throws IOException {


			String input = value.toString();



			//System.out.println(input + "\n ################");

			//for position: if in document there is 13-15, in java, we write 12-15.
			//the first changes because in java the index begin at 0 and in document at 1.
			//the second changes because in java, substring(a,b) is: from a included to b excluded


			//First check if there enough data.
			//We need at least the snowDepth (AJ1) or precipitation (AA1). Otherwise,
			// it is not possible to compute anything
			if (input.contains("AJ1") || input.contains("AA1")) {


				String stationID = input.substring(4, 10);

				//get and format date (for the key)
				Calendar c = Calendar.getInstance();
				c.set(Integer.parseInt(input.substring(15, 19)), //year
						Integer.parseInt(input.substring(19, 21)),  //month
						Integer.parseInt(input.substring(21, 23)), //day
						Integer.parseInt(input.substring(23, 25)), //hour
						Integer.parseInt(input.substring(25, 27))); //minutes
				SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd-HH:mm");

				String time = dateFormatter.format(c.getTime());

				String outKey = stationID + "\t" + time;


				String tempString = input.substring(87, 92);

				//output.collect(new Text("hey" + a), new IntWritable(2));

				//access data that are not mandatory in the reports.
				int snowDepth = -1;
				int pastWeather = -1;
				int presentWeather = -1;

				final String PRESENT_WEATHER_OBSERVATION_SNOW = "03";
				final String PRESENT_WEATHER_OBSERVATION_AUTOMATED_SNOW_GRAIN = "04";


				boolean cumulationComputedWithSnowDepth = false;

				//Snow depth is given
				if (input.contains("AJ1")) {
					//System.out.println(input);
					int startPosition = input.indexOf("AJ1") + 3; //start of depth value AJ1xxxx, xxxx is the depth
					int endPosition = startPosition + 4;


					try {
						int newDepth = Integer.parseInt(input.substring(startPosition, endPosition));

						int depthDiff = newDepth - lastDepth;

						cumulationComputedWithSnowDepth = true;
					} catch (NumberFormatException e) {

					}

				}


				//estimate
				if (input.contains("AA1")) {
					//measure every hour
					//output existing snow data
					//utput.collect(new Text(stationID), new IntWritable(1));
					int startPosition = input.indexOf("AA101");
					int endPosition = startPosition + 10;

					String precipitationData = input.substring(startPosition, endPosition);

					//Which weather? Can help find snow condition between -1 and 3 °C, where it can rain or snow (or  both)
					//See ish-format-document.pdf pages 26 to 32
					String precipitationType = ""; // TODO

					if(input.contains(""))



					float precipitationAmount = Float.parseFloat(precipitationData.substring(5, 9));
					int precipitationMeasureInterval = Integer.parseInt(precipitationData.substring(3, 5));

				}*/



				//estimation


/*			if (input.contains("AA1")) {
				//measure every hour
				//output existing snow data
				//utput.collect(new Text(stationID), new IntWritable(1));
				int startPosition = input.indexOf("AA101");
				int endPosition = startPosition + 10;

				String precipitationData = input.substring(startPosition, endPosition);

				//Which weather? Can help find snow condition between -1 and 3 °C, where it can rain or snow (or  both)
				//See ish-format-document.pdf pages 26 to 32
				String precipitationTypePresentAutomated = ""; // TODO
				String precipitationTypePresentManual = ""; // TODO
				String precipitationTypePastManual = ""; // TODO
				String precipitationTypePastAutomated = ""; // TODO


				float precipitationAmount = Float.parseFloat(precipitationData.substring(5, 9));
				int precipitationMeasureInterval = Integer.parseInt(precipitationData.substring(3, 5));

			}*/


				//no precipitation data


				//output.collect(new Text(stationID + c.toString()), new Text());

			}

		}

		public float estimateSnowFall(float temperature, float hourlyPrecipitation, int precipitationType) {
			//will use or temperature, or precipitation type to see if it is snow.
			//then will compute the equivalent amount of snow based on the temperature

			return 0;
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
		                   Reporter reporter) throws IOException {

            /*
             * Each key-value pair is of kind: "year-month" - {43, 50, 60, 30, 20, 0}. Values are all measured depth
             * in the time period given by the key
             */


			//


			int nb = 0;

			while (values.hasNext()) {
				values.next();
				nb++;
			}

			output.collect(key, new Text(nb + ""));


		}


	}


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		//Path p = new Path(args[1]);
		Path outPath = new Path(args[1]);
		Path inPath = new Path(args[0]);


		JobConf conf1 = new JobConf(SnowFallEstimation.class);
		conf1.setJobName("find snow stations");

		conf1.setMapperClass(MapPreprocessing.class);
		conf1.setReducerClass(Reduce.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		conf1.setNumMapTasks(2);
		conf1.setNumReduceTasks(2);

		conf1.setMapOutputKeyClass(Text.class);
		conf1.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf1, inPath);
		FileOutputFormat.setOutputPath(conf1, outPath);


		JobClient.runJob(conf1);
	}


}


class data {
	public final float NO_TEMPERATURE_PROVIDED = 30000f; //just need a fake number to notify that there is no
	// temperature that is provided (due to an error)
	public final int NO_SNOW_DEPTH_PROVIDED = -1;
	public final int NO_SNOW_FALL_PROVIDED = -1;

	private String snowDepth;
	private String snowFallFromSnowDepth;
	private String snowFallFromRain;

	public data(String snowDepth, String snowFalFromDepth,
	            String snowFallFromRain) {
		try {

			if (NumberUtils.isNumber(snowDepth)) {
				this.snowDepth = snowDepth;
			} else {
				//no temperature provided, set it to 30000;
				this.snowDepth = "**";
			}

			if (NumberUtils.isNumber(snowFalFromDepth)) {
				this.snowFallFromSnowDepth = snowFalFromDepth;
			} else {
				//no temperature provided, set it to 30000;
				this.snowFallFromSnowDepth = "**";
			}

			if (NumberUtils.isNumber(snowFallFromRain)) {
				this.snowFallFromRain = snowFallFromRain;
			} else {
				//no temperature provided, set it to 30000;
				this.snowFallFromRain = "**";
			}
		} catch (NumberFormatException e) {

		}
	}
}