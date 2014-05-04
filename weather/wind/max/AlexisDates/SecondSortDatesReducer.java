package weather.wind.max;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultimap;

public class SecondSortDatesReducer extends MapReduceBase implements
Reducer<IntWritable, Text, IntWritable, Text>
{

	@Override
	public void reduce(IntWritable inputKey, Iterator<Text> inputValue,
			OutputCollector<IntWritable, Text> output, Reporter arg3) throws IOException {

		Multimap<Integer, String> datesSorted = TreeMultimap.create();
		while (inputValue.hasNext())
		{
			String inputValueString = inputValue.next().toString();
			Integer date = Integer.parseInt(inputValueString.substring(11, 19));
			datesSorted.put(date, inputValueString);
		}
		//datesSorted.get(20110412);

		Multiset<Integer> keys = datesSorted.keys();
		System.out.println("Keys Length : " + keys.size());
		Collection<String> values = datesSorted.values();

		ArrayList<Integer> keysList = new ArrayList<Integer>();
		for(Integer key : keys) {
			keysList.add(key);
			System.out.println("Key : " + key);
		}
		System.out.println("Chiavi Length : " + keysList.size());

		ArrayList<String> valuesList = new ArrayList<String>();
		for(String val : values) {
			valuesList.add(val);
		}
		System.out.println("Valori Length : " + valuesList.size());

		ArrayList<Integer> consecutives = new ArrayList<Integer>();

		int flag = 0;
		for (int i = 0; i < keysList.size(); i++) {
			if(consecutives.isEmpty()){
				consecutives.add(keysList.get(i));	
				System.out.println("Elemento Singolo :"+consecutives.get(0));
			}
			else if (!consecutives.isEmpty() && (keysList.get(i)-consecutives.get(consecutives.size()-1)<= 1)){
				consecutives.add(keysList.get(i));
				if (keysList.get(i+1)-consecutives.get(consecutives.size()-1) > 1){
					int l = consecutives.size();
					//take the largest consecutive value as key
					IntWritable outputKey = new IntWritable(consecutives.get(l-1));
					for(int j = 0; j < l; j++){
						Text outputValue = new Text(valuesList.get(flag+j).toString());
						System.out.println(outputKey + "     value: " + outputValue);
						output.collect(outputKey,outputValue);
					}
					flag += l;
					consecutives.clear();
				}
			}
		}
		int l = consecutives.size();
		System.out.println("L : "+l);
		//take the largest consecutive value as key
		IntWritable outputKey = new IntWritable(consecutives.get(l-1));
		for(int i = 0; i < l; i++){
			Text outputValue = new Text(valuesList.get(flag+i).toString());
			System.out.println(outputKey + "     value: " + outputValue);
			output.collect(outputKey,outputValue);
		}
	}
}