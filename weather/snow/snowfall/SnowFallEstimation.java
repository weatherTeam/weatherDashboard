package weather.snow.snowfall;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;


/**
 * Created by Jonathan Duss on 01.04.14.
 */
public class SnowFallEstimation {



	/**
		Preprocess the data: if snow depth is given, it may be measured each XX hours.

		output:
		key: stationID_year/month/day
		value: temperature  snowDepth snow_cum_from_depth rain snowDepth snow_cum_from_rain
	 */
	public static class MapperDailySnowComputation extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		int lastDepth = -1;
		//number of hours since the s
		int lastDepthTime = 0;

		//After an amount of time, if there is no value, we consider there is no more snow
		// (generally, if there is no measure after some time, it means there is no more snow at all)
		final int SNOW_DEPTH_DISCARD_TIME = 48;

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
		                Reporter reporter) throws IOException {


			String input = value.toString();

			//increase counter
			lastDepthTime++;
			//We consider no more snow, so snowDepth = 0
			if( lastDepthTime > SNOW_DEPTH_DISCARD_TIME){
				lastDepth = 0;
			}

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
			if ((input.indexOf("AJ1") != -1) || (input.indexOf("AA1") != -1)) {


				String stationID = input.substring(4, 10);

				//get and format DATE (for the key)
				Calendar c = Calendar.getInstance();
				c.set(Integer.parseInt(input.substring(15, 19)), //year
						Integer.parseInt(input.substring(19, 21)),  //month
						Integer.parseInt(input.substring(21, 23)), //day
						Integer.parseInt(input.substring(23, 25)), //hour
						Integer.parseInt(input.substring(25, 27))); //minutes
				SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/WW"); //"yyyy/MM/dd-HH:mm"

				String time = dateFormatter.format(c.getTime());

				String outKey = stationID + "\t" + time;


				String tempString = input.substring(87, 92);
				float temperature = 0;
				try {
					temperature = Float.parseFloat(tempString) / 10; //divide by 10 because of scaling factor (see ish)
				} catch (NumberFormatException e) {
					SnowData d;
					temperature = SnowData.NO_TEMPERATURE_PROVIDED;
				}




				int cumulationFromSnowDepth = SnowData.NO_SNOW_INFO;
				float cumulationFromRain = SnowData.NO_SNOW_INFO;
				int snowDepth = SnowData.NO_SNOW_INFO;
				float precipitationAmount = SnowData.NO_SNOW_INFO;

				//SNOW_DEPTH is given: compare with the last known snow depth
				if (input.indexOf("AJ1") != -1) {
					//System.out.println("AJ1");
					int startPosition = input.indexOf("AJ1") + 3; //start of depth value AJ1xxxx, xxxx is the depth
					int endPosition = startPosition + 4;


					try {
						System.out.println("SNOW DEPTH : " + input.substring(startPosition, endPosition));
						int newDepth = Integer.parseInt(input.substring(startPosition, endPosition));
						snowDepth = newDepth;

						if (lastDepth != SnowData.NO_SNOW_INFO) {
							cumulationFromSnowDepth = newDepth - lastDepth;

							//snow cumulation is >= 0, otherwise, it is only melt, which should not decrease the snow
							// cumulation. We just update the lastSnowDepth value
							//TODO: if data starts from lets say 1st september, then we can consider snow depth = 0
							//if start from 1st january, then we do not consider the first day, which will serve as an initial value
							if (cumulationFromSnowDepth >= 0) {
								containsDataFromSnowDepth = true;
							}
						}
						lastDepth = newDepth;
						lastDepthTime = 0;

					} catch (NumberFormatException e) {
						cumulationFromSnowDepth = SnowData.NO_SNOW_INFO;
						lastDepth = SnowData.NO_SNOW_INFO;
					}

				}


				//RAIN: AA1
				//estimate from rain (AA1xx, is rain, measure during interval xx hours
				//unity: mm
				if (input.indexOf("AA101") != -1 || input.indexOf("AA102") != -1 || input.indexOf("AA101") != -1) {

					//Which weather? Can help find snow condition between -1 and 3 °C, where it can rain or snow (or
					// both)
					//See ish-format-document.pdf pages 26 to 32
					String precipitationType = "";

					if (input.contains("AU1")) {
						int startPos = input.indexOf("AU1") + 3 + 1 + 1;
						precipitationType = input.substring(startPos, startPos + 2);
					}


					if (precipitationType.equals("03") ||
							precipitationType.equals("04") ||
							precipitationType.equals("05") ||
							precipitationType.equals("08") ||
							precipitationType.equals("")) {
						//If the observation say it snow, or there is no observation, it is possible that it is snowing
						//if there is precipitation.
						//In the other case, we know for sure it is not snowing

						try {
							int indexOfAA1 = input.indexOf("AA1");
							String precipitationString = input.substring(indexOfAA1 + 3 + 2, indexOfAA1 + 3 + 2 + 4);
							//fix temperature according to the conversion table which only convert if temperature is
							// under 1.1. So if it the weather observation says it is snowing and the temperature is 2
							// (or more general, is over 1.1), we set it to 1.1
							if ((precipitationType.equals("03") ||
									precipitationType.equals("04") ||
									precipitationType.equals("05") ||
									precipitationType.equals("08")) && temperature > 1.1){
								temperature = 1.1f;
							}
							precipitationAmount = Float.parseFloat(precipitationString) / 10;
							cumulationFromRain = estimateSnowFall(temperature, precipitationAmount);
							containsDataFromRain = cumulationFromRain > 0;
						} catch (NumberFormatException e) {
							cumulationFromRain = SnowData.NO_SNOW_INFO;
							precipitationAmount = SnowData.NO_SNOW_INFO;
						}
					}


				} //end if (input.contains("AA101") || input.contains("AA102") || input.contains("AA101"))

				if (containsDataFromSnowDepth || containsDataFromRain) {
					SnowData sd = new SnowData();
					sd.setSnowDepth(snowDepth);
					sd.setSnowFallFromRain(cumulationFromRain);
					sd.setSnowFallFromSnowDepth(cumulationFromSnowDepth);
					sd.setPrecipitation(precipitationAmount);
					sd.setTemperature(temperature);

					output.collect(new Text(outKey), new Text(sd.toString()));

					//System.out.println(outKey + "\t" + input.contains("AJ1") + "\t" + input.contains("AA1"));
					//System.out.println(sd.toString());
				}

			} //end if (input.contains("AJ1") || input.contains("AA1"))

		}



		/*
			Estimate how much snow fall during the last precipitation, knowing the amount in water
		 */
		public float estimateSnowFall(float temperature, float precipitationAmount) {
			//will use or temperature, or precipitation type to see if it is snow.
			//then will compute the equivalent amount of snow based on the temperature

			//ration rain->snow for the given temperature:
			// 1.1 to -2.2	    => 10
			// -2.2 to -6.7     => 15
			// -6.7 to -9.4     => 20
			// -9.4 to -12.2    => 30
			// 	-12.2 t -17.8   => 40
			// -17.8 to -28.9   => 50
			// 	-28.9 to -40    => 100
			//lower => suppose also 100 (but this case should be rare)
			float tempTest[] = {1.1f, -2.2f, -6.7f, -9.4f, -12.2f, -17.8f, -29};
			int ratioTab[] = {10, 15, 20, 30, 40, 50, 100};
			int ratio = 0;
			for (int i = 0; i < tempTest.length; i++) {
				if (temperature <= tempTest[i]) {
					ratio = ratioTab[i];
				}
			}

			return precipitationAmount * ratio;

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
			float total = 0;
			float total2 = 0;

			while (values.hasNext()) {
				SnowData d = new SnowData(values.next().toString());
				total2 += d.getSnowFallFromRain();
				total += d.getSnowFallFromSnowDepth();
			}

			output.collect(outKey, new Text("" + total + " " + total2));


		}


	}


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		SnowData d2;

		//Path p = new Path(args[1]);
		Path outPath = new Path(args[1]);
		Path inPath = new Path(args[0]);


		JobConf conf1 = new JobConf(SnowFallEstimation.class);
		conf1.setJobName("find snow stations");

		conf1.setMapperClass(MapperDailySnowComputation.class);
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


