package weather.rain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgMonthMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();

	@Override
	public void map(LongWritable inputKey, Text inputValue, Context context)
			throws IOException, InterruptedException {
		String line = inputValue.toString();

		if(!line.substring(17,21).equals("PRCP"))
			return;

		String stationID = line.substring(0, 11);
		String yearStr = line.substring(11,15);
		String monthStr = line.substring(15,17);
		int year = Integer.parseInt(yearStr);

		int beginyear = context.getConfiguration().getInt("beginyear", 1900);
		int endyear = context.getConfiguration().getInt("endyear", 2020);

		if(year > endyear || year < beginyear)
			return;
		int startIndex = 21;
		int step = 8;
		for(int i = 0 ; i < 31 ; i++){
			int index = startIndex+step*i;
			String rainAmountStr = line.substring(index,index+5);
			@SuppressWarnings("unused")
			char MFLAG = line.charAt(index+5);
			char QFLAG = line.charAt(index+6);
			@SuppressWarnings("unused")
			char SFLAG = line.charAt(index+7);
			int rainAmount = Integer.parseInt(rainAmountStr.trim());
			if(rainAmount > 0 && rainAmount < 9999 && QFLAG == ' '){
				outputKey.set(stationID+","+monthStr);
				outputValue.set(rainAmount);
				context.write(outputKey, outputValue);
			}
		}

	}

}