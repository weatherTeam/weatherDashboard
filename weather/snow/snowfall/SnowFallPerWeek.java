package weather.snow.snowfall;

/**
 * Created by Jonathan Duss on 26.04.14.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;



/**
 * Created by Jonathan Duss on 01.04.14.
 */
public class SnowFallPerWeek {


	/**
	 * Preprocess the data: if snow depth is given, it may be measured each XX hours.
	 * <p/>
	 * output:
	 * key: stationID_year/month/day
	 * value: temperature  snowDepth snow_cum_from_depth rain snowDepth snow_cum_from_rain
	 */
	public static class MapperDailySnowComputation extends MapReduceBase implements Mapper<LongWritable, Text, Text,
			Text> {

		int lastDepth = -1;

		//store the last time they was information about snow depth.
		Calendar lastDepthTime = Calendar.getInstance();
		final long MILIS_IN_2_DAYS = 2 * 24 * 3600 * 1000;

		//After an amount of time, if there is no value, we consider there is no more snow.
		// (generally, if there is no measure after some time, it means there is no more snow at all)
		// generally the snow depth is measured 2 times a day
		final int SNOW_DEPTH_DISCARD_TIME = 48;

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
		                Reporter reporter) throws IOException {


			String input = value.toString();


			//as soon as data are computed / found, it is set to true
			//This is used to avoid emiting a key/value in case all string to number conversions fail.
			boolean containsDataFromSnowDepth = false;
			boolean containsDataFromRain = false;

			//for position: if in document there is 13-15, in java, we write 12-15.
			//the first changes because in java the index begin at 0 and in document at 1.
			//the second changes because in java, substring(a,b) is: from a included to b excluded


			//First check if there enough data. (Need snowdepth OR precipitation amount)
			//We need at least the snowDepth (AJ1) or precipitation (AA1). Otherwise,
			// it is not possible to compute anything


				String stationID = input.substring(4, 10);

				//location of the station
				String latitude = input.substring(28,34);
				String longitude = input.substring(24,41);


				//read and format DATE
				Calendar c = Calendar.getInstance();
				SimpleDateFormat dateFormatterReader = new SimpleDateFormat("yyyyMMddHHmm");
				Date date = dateFormatterReader.parse(input.substring(15, 27), new ParsePosition(0));
				c.setTime(date);

				//After 2 days without any news, we consider no more snow, so snowDepth = 0
				if(c.getTimeInMillis() - lastDepthTime.getTimeInMillis() > MILIS_IN_2_DAYS){
					lastDepth = 0;
				}


				//format date that will use for the key
				SimpleDateFormat dateFormatterForKey = new SimpleDateFormat("yyyy/MM/WW"); //"yyyy/MM/dd-HH:mm"
				SimpleDateFormat dateFormatterForSyso = new SimpleDateFormat("yyyy/MM/dd-HH:mm"); // for syso

				String time = dateFormatterForKey.format(c.getTime());
				String outKey = stationID + "\t"+latitude + "\t" + longitude + "\t" + time;








		}



	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
		                   Reporter reporter) throws IOException {

            /*
             * Each key-value pair is of kind: "year-month" - {43, 50, 60, 30, 20, 0}. Values are all measured depth
             * in the time period given by the key
             */
			Text outKey = key;
			float totalFromSnowDepth = 0;
			float totalFromRain = 0;

			boolean containSnowFallFromRain = false;
			boolean containsSnowFallFromSnowDepth = false;

			while (values.hasNext()) {
				SnowData d = new SnowData(values.next().toString());

				if(d.getSnowFallFromRain() != SnowData.NO_SNOW_INFO){
					totalFromRain += d.getSnowFallFromRain();
					containSnowFallFromRain = true;
				}
				if(d.getSnowFallFromSnowDepth() != SnowData.NO_SNOW_INFO) {
					totalFromSnowDepth += d.getSnowFallFromSnowDepth();
					containsSnowFallFromSnowDepth = true;
				}
			}

			//final: round snow cumulation to integer value
			DecimalFormat df = new DecimalFormat("###"); //

			//for debug to test accuracy for station where rain and snow depth is known, so it is possible to see if the
			// cumulation computed using the snowdepth is same as the one computed with rain
			//output.collect(outKey, new Text("" + df.format(total) + " " + df.format(total2)));

			//Priority: use data from snow depth if exists (more precise), otherwise, use the one computed from rain
			if(containsSnowFallFromSnowDepth){
				output.collect(outKey, new Text("" + df.format(totalFromSnowDepth)));
			}
			else if(containSnowFallFromRain){
				output.collect(outKey, new Text("" + df.format(totalFromRain)));
			}

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


		JobConf conf1 = new JobConf(DailySnowFallEstimation.class);
		conf1.setJobName("find snow stations");

		conf1.setMapperClass(MapperDailySnowComputation.class);
		conf1.setReducerClass(Reduce.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		conf1.setNumMapTasks(20);
		conf1.setNumReduceTasks(20);

		conf1.setMapOutputKeyClass(Text.class);
		conf1.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(conf1, inPath);
		FileOutputFormat.setOutputPath(conf1, outPath);


		JobClient.runJob(conf1);


	}


}



