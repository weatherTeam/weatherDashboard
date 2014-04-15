package weather.snow.snowfall;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


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

			//as soon as data are computed / found, it is set to true
			//This is used to avoid emiting a key/value in case all string to number conversions fail.
			boolean containsData = false;

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
				SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd-HH:mm");

				String time = dateFormatter.format(c.getTime());

				String outKey = stationID + "\t" + time;


				String tempString = input.substring(87, 92);
				float temperature = 0;
				try {
					temperature = Float.parseFloat(tempString) / 10; //divide by 10 because of scaling factor (see ish)
				} catch (NumberFormatException e) {
					temperature = SnowData.NO_TEMPERATURE_PROVIDED;
				}


				//access data that are not mandatory in the reports.


				int cumulationFromSnowDepth = SnowData.NO_SNOW_INFO;
				int cumulationFromRain = SnowData.NO_SNOW_INFO;
				int snowDepth = SnowData.NO_SNOW_INFO;
				float precipitationAmount = SnowData.NO_SNOW_INFO;

				//Snow depth is given: compare with the last known snow depth
				if (input.contains("AJ1")) {
					//System.out.println(input);
					int startPosition = input.indexOf("AJ1") + 3; //start of depth value AJ1xxxx, xxxx is the depth
					int endPosition = startPosition + 4;


					try {
						int newDepth = Integer.parseInt(input.substring(startPosition, endPosition));

						cumulationFromSnowDepth = newDepth - lastDepth;
						containsData = true;

					} catch (NumberFormatException e) {
						cumulationFromSnowDepth = SnowData.NO_SNOW_INFO;
					}

				}

				//RAIN: AA1
				//estimate from rain (AA1xx, is rain, measure during interval xx hours
				//unity: mm

				if (input.contains("AA101") || input.contains("AA102") || input.contains("AA101")) {

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
							precipitationAmount = Float.parseFloat(
									input.substring(indexOfAA1 + 3 + 2, indexOfAA1 + 3 + 2 + 4)) / 10;
							cumulationFromRain = estimateSnowFall(temperature, precipitationAmount);
							containsData = true;
						} catch (NumberFormatException e) {
							cumulationFromRain = SnowData.NO_SNOW_INFO;
							precipitationAmount = SnowData.NO_SNOW_INFO;
						}
					}


				} //end if (input.contains("AA101") || input.contains("AA102") || input.contains("AA101"))

				if(containsData) {
					SnowData sd = new SnowData();
					sd.setSnowDepth(snowDepth);
					sd.setSnowFallFromRain(cumulationFromRain);
					sd.setSnowFallFromSnowDepth(cumulationFromSnowDepth);
					sd.setPrecipitation(precipitationAmount);
					sd.setTemperature(temperature);

					output.collect(new Text(outKey), new Text(sd.toString()));

					System.out.println(outKey + "\t" + input.contains("AJ1") + "\t" + input.contains("AA1"));
				}

			} //end if (input.contains("AJ1") || input.contains("AA1"))

		}


		/*
			Estimate how much snow fall during the last precipitation, knowing the amount in water
		 */
		public int estimateSnowFall(float temperature, float precipitationAmount) {
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
			float tempTest[] = {1.1f, -2.2f, -6.7f, -9.4f, -12.2f, -17.8f, -29, -40};
			int ratioTab[] = {0, 10, 15, 20, 30, 40, 50, 100};
			int ratio = 0;
			for (int i = 0; i < tempTest.length; i++) {
				if (temperature < tempTest[i]) {
					ratio = ratioTab[i];
				}
			}

			return Math.round(precipitationAmount * ratio);

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
		                   Reporter reporter) throws IOException {

            /*
             * Each key-value pair is of kind: "year-month" - {43, 50, 60, 30, 20, 0}. Values are all measured depth
             * in the time period given by the key
             */
			int nb = 0;

			while (values.hasNext()) {
				Text v = values.next();
				nb++;
				output.collect(key, v);
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


/*
	Class storing snow data, and allowing to quickly pass these data through the mapper into a "standard" format and
	recover quickly in the reducer
 */
class SnowData {
	public final static float NO_TEMPERATURE_PROVIDED = 30000f; //just need a fake number to notify that there is no
	// temperature that is provided (due to an error)
	public final static int NO_SNOW_INFO = -1;
	public final int NO_SNOW_FALL_PROVIDED = -1;

	private String snowDepth;
	private String snowFallFromSnowDepth;
	private String snowFallFromRain;
	private String precipitation;
	private String temperature;

	//CONSTRUCTOR

//	public SnowData(String snowDepth, String snowFalFromDepth,
//	                String snowFallFromRain) {
//		try {
//
//			if (NumberUtils.isNumber(snowDepth)) {
//				this.snowDepth = snowDepth;
//			} else {
//				//no temperature provided, set it to 30000;
//				this.snowDepth = "**";
//			}
//
//			if (NumberUtils.isNumber(snowFalFromDepth)) {
//				this.snowFallFromSnowDepth = snowFalFromDepth;
//			} else {
//				//no temperature provided, set it to 30000;
//				this.snowFallFromSnowDepth = "**";
//			}
//
//			if (NumberUtils.isNumber(snowFallFromRain)) {
//				this.snowFallFromRain = snowFallFromRain;
//			} else {
//				//no temperature provided, set it to 30000;
//				this.snowFallFromRain = "**";
//			}
//		} catch (NumberFormatException e) {
//
//		}
//	}

	public SnowData() {
	}


	//GETTER AND SETTER
	public void setSnowDepth(int sn) {
		if (sn < 0) {
			snowDepth = "**";
		} else {
			snowDepth = "" + sn;
		}
	}

	public void setSnowFallFromSnowDepth(int sn) {
		if (sn < 0) {
			snowFallFromSnowDepth = "**";
		} else {
			snowFallFromSnowDepth = "" + sn;
		}
	}

	public void setSnowFallFromRain(int sn) {
		if (sn < 0) {
			snowFallFromRain = "**";
		} else {
			snowFallFromRain = "" + sn;
		}
	}

	public String getTemperature() {
		return temperature;
	}

	public void setTemperature(String temperature) {
		this.temperature = temperature;
	}

	public void setTemperature(float temp) {
		if (temp < 0) {
			temperature = "**";
		} else {
			temperature = "" + temp;
		}
	}

	public String getSnowDepth() {
		return snowDepth;
	}

	public void setSnowDepth(String snowDepth) {
		this.snowDepth = snowDepth;
	}

	public String getSnowFallFromSnowDepth() {
		return snowFallFromSnowDepth;
	}

	public void setSnowFallFromSnowDepth(String snowFallFromSnowDepth) {
		this.snowFallFromSnowDepth = snowFallFromSnowDepth;
	}

	public String getSnowFallFromRain() {
		return snowFallFromRain;
	}

	public void setSnowFallFromRain(String snowFallFromRain) {
		this.snowFallFromRain = snowFallFromRain;
	}

	public String getPrecipitation() {
		return precipitation;
	}

	public void setPrecipitation(String precipitation) {
		this.precipitation = precipitation;
	}

	public void setPrecipitation(float p) {
		if (p < 0) {
			this.precipitation = "**";
		} else {
			this.precipitation = "" + p;
		}
	}


	public String toString() {
		return temperature + "\t" + snowDepth + "\t" + snowFallFromSnowDepth + "\t" + precipitation + " \t" +
				snowFallFromRain;
	}


}