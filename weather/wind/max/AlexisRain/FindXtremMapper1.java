package weather.wind.max;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FindXtremMapper1 extends MapReduceBase implements
Mapper<LongWritable, Text, IntWritable, Text>
{
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


		String mmPrcpString = "9999";
		String prcpQualityCode;

		if(inputValue.toString().indexOf("AA1") != -1)
		{
			try{
				int startIndex = inputValue.toString().indexOf("AA1")+5;
				int endIndex = startIndex + 4;
				mmPrcpString = inputValue.toString().substring(startIndex, endIndex).trim();
//				System.out.println("PRCP STRING : " + mmPrcpString);
//				prcpQualityCode = inputValue.toString().substring(startIndex+5,startIndex+6);
			} catch (StringIndexOutOfBoundsException e){
				System.out.println("String out of Bound");
			}			
		}


		if (
				Integer.parseInt(windString) > 0
				&& Integer.parseInt(windString) != MISSING
				//&& Integer.parseInt(mmPrecString) != MISSING
				&& windQuality.matches("[01459]"))
				//&& prcpQualityCode.matches("[01459]"))
		{
			int wind = Integer.parseInt(windString);
			int mmPrcp = 9999;
			try {
				mmPrcp = Integer.parseInt(mmPrcpString);
			}catch (NumberFormatException e){
				System.out.println("Precipitation not valid :"+mmPrcpString);
			}
//			int mmPrcp = Integer.parseInt(mmPrcpString);
			if (wind < 10)
				windString = "000" + wind;
			else if (wind < 100)
				windString = "00" + wind;
			else
				windString = "0"+wind;

			if (mmPrcp < 10)
				mmPrcpString = "000" + mmPrcp;
			else if (mmPrcp < 100)
				mmPrcpString = "00" + mmPrcp;
			else if (mmPrcp < 1000)
				mmPrcpString = "0" + mmPrcp;
			else
				mmPrcpString = "" + mmPrcp;



			IntWritable outputKey = new IntWritable(Integer.parseInt(month));
			Text outputValue = new Text(id + date + geoloc + windString + mmPrcpString);	
			output.collect(outputKey, outputValue);
		}

		// the output key is by monthes
		// the output value contains all informations (id + date + geoloc +
		// wind)
					}
}

