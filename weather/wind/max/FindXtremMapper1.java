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
	private static final int MISSING = 9999;

	@Override
	public void map(LongWritable inputKey, Text inputValue,
			OutputCollector<IntWritable, Text> output, Reporter arg3)
			throws IOException
	{

		// extraction of the needed information from the raw string.
		String windQuality = inputValue.toString().substring(69, 70);
		String id = inputValue.toString().substring(4, 15);
		String date = inputValue.toString().substring(15, 27);
		String month = (date.substring(4, 6));
		String geoloc = inputValue.toString().substring(28, 41);
		String windString = inputValue.toString().substring(65, 69);

		// 9999 indicates a missing value.
		String mmPrcpString = "9999";

		// if AA1 is present in the string it means there is rain.
		if (inputValue.toString().indexOf("AA1") != -1)
		{
			try
			{
				int startIndex = inputValue.toString().indexOf("AA1") + 5;
				int endIndex = startIndex + 4;
				mmPrcpString = inputValue.toString()
						.substring(startIndex, endIndex).trim();
			}
			catch (StringIndexOutOfBoundsException e)
			{
				System.out.println("String out of Bound");
			}
		}

		// the quality factors indicate the quality of the informations
		// so we are sure we use only good data.
		if (Integer.parseInt(windString) > 0
				&& Integer.parseInt(windString) != MISSING
				&& windQuality.matches("[01459]"))
		{
			int wind = Integer.parseInt(windString);
			int mmPrcp = 9999;
			
			try
			{
				mmPrcp = Integer.parseInt(mmPrcpString);
			}
			catch (NumberFormatException e)
			{
				System.out.println("Precipitation not valid :" + mmPrcpString);
			}

			windString = formatWind(wind);
			mmPrcpString = formatPrecipitation(mmPrcp);

			IntWritable outputKey = new IntWritable(Integer.parseInt(month));
			Text outputValue = new Text(id + date + geoloc + windString + mmPrcpString);
			
			output.collect(outputKey, outputValue);				
		}
	}
	
	public String formatWind(int wind)
	{
		String windString;
		
		if (wind < 10)
			windString = "000" + wind;
		else if (wind < 100)
			windString = "00" + wind;
		else
			windString = "0" + wind;
		
		return windString;
	}
	
	public String formatPrecipitation(int mmPrcp)
	{
		String mmPrcpString;
		
		if (mmPrcp < 10)
			mmPrcpString = "000" + mmPrcp;
		else if (mmPrcp < 100)
			mmPrcpString = "00" + mmPrcp;
		else if (mmPrcp < 1000)
			mmPrcpString = "0" + mmPrcp;
		else
			mmPrcpString = "" + mmPrcp;
		
		return mmPrcpString;
	}
}
