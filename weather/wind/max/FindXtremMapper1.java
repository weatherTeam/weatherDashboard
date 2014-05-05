package weather.wind.max;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FindXtremMapper1 extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, Text>
{
	
	/**
	 * FOR ALEXIS:
	 * So, just make the rain works. You only have to work in this mapper.
	 * I would like:
	 * 		precipitationPerHour = 4 digits
	 * 		mmPrec = 6 digits
	 * so I can make my last mapRed work on it.
	 * I already fixed the 1st reducer by deleting all the useless line.
	 * So, you also output suff from this mapper if there is no rain. Because
	 * we can have extrem winds without rain!
	 * If there is no rain just put 0000000000 at the end, I will deal with it.
	 * The info will be : "rain precipitation: 0mm" and that's it.
	 * 
	 * Write if you have any question.
	 */
	
	private static final int MISSING = 9999;
//	private static final int MISSING99 = 99;
	
	@Override
	public void map(LongWritable inputKey, Text inputValue,
			OutputCollector<IntWritable, Text> output, Reporter arg3)
			throws IOException
	{

		String windQuality = inputValue.toString().substring(69, 70);
		String id = inputValue.toString().substring(4, 15);
		String date = inputValue.toString().substring(15, 27);
		String month = (date.substring(4, 6));
		String geoloc = inputValue.toString().substring(28, 41);
		String windString = inputValue.toString().substring(65, 69);

//		String precipitationPerHourString = null;
//		String mmPrecString = null;
//
//		if (inputValue.toString().contains("AA1"))
//		{
//			int startIndex = inputValue.toString().indexOf("AA1");
//			int endIndex = startIndex + 10;
//			String precipitation = inputValue.toString().substring(startIndex, endIndex);
//			precipitationPerHourString = precipitation.substring(3, 5).trim();
//			mmPrecString = precipitation.substring(5, 9).trim();
//		}
		

		if (
				Integer.parseInt(windString) > 0
//				&& precipitationPerHourString != null & mmPrecString != null
				&& Integer.parseInt(windString) != MISSING
//				&& Integer.parseInt(mmPrecString) != MISSING
//				&& Integer.parseInt(precipitationPerHourString) != MISSING99
				&& windQuality.matches("[01459]"))
		{
			int wind = Integer.parseInt(windString);
//			int mmPrec = Integer.parseInt(mmPrecString);
//			int precPerHour = Integer.parseInt(precipitationPerHourString);
			
			if (wind < 10)
				windString = "000" + wind;
			else if (wind < 100)
				windString = "00" + wind;
			else
				windString = "0"+wind;
			
//			if (mmPrec < 10)
//				mmPrecString = "00" + mmPrec;
//			else if (mmPrec < 100)
//				mmPrecString = "0" + mmPrec;
//			
//			if (precPerHour < 10)
//				precipitationPerHourString = "00" + precPerHour;
//			else if (precPerHour < 100)
//				precipitationPerHourString = "0" + precPerHour;
			
			
			IntWritable outputKey = new IntWritable(Integer.parseInt(month));
			Text outputValue = new Text(id + date + geoloc + windString);
//					+ precipitationPerHourString
//					+ mmPrecString);			
			output.collect(outputKey, outputValue);
		}

		// the output key is by monthes
		// the output value contains all informations (id + date + geoloc +
		// wind)
	}
}
