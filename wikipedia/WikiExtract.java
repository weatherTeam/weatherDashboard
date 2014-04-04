
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;


/*For a more detailed explanation see wiki: https://github.com/weatherTeam/weatherDashboard/wiki/Wikipedia*/
public class WikiExtract {

//A list of useful keywords that will define if an article is relevant or not (if the category contains one of those words).
public static String[] months = {"january","february","march", "april" ,"may","june","july","august","september","october", "november", "december"};

//Regex to check if category contains one of the keywords
public static Pattern monthRegex = createRegexForMonths();

//Regex for years
public static Pattern yearRegex = Pattern.compile(".*(\\d\\d\\d\\d).*");

//Regex to extract title from article
public static Pattern titleRegex = Pattern.compile("<title>(.*)</title>");

//Regex for countries/continents
public static Pattern countryRegex = createRegexForCountries();


//Create a regex of the form ".*(keyword1|keyword2|...|keywordn).*"
static Pattern createRegexForMonths(){
	String regex = ".*(";
	
	for(int i = 0; i < months.length - 1; i++){
		regex += months[i]+"|";
	}
	regex += months[months.length - 1];
	regex = regex + ").*";
	
	return Pattern.compile(regex);
}


//Create a regex to extract country, from a predifined list
static Pattern createRegexForCountries(){
	String regex = ".*(";
	
	//TODO
	
	String countryFile = "resources/demonyms.txt";
	BufferedReader br = null;
	String line = "";
	
	try {
		br = new BufferedReader(new FileReader(countryFile));
		while ((line = br.readLine()) != null) {
			line = line.toLowerCase();
			String countryAndDemonym[] = line.split("\t");
			if(countryAndDemonym.length > 1){
				regex += countryAndDemonym[0]+"|"+countryAndDemonym[1]+"|";
			}
		}
		
		
	} catch (Exception e) {
		e.printStackTrace();
		System.out.println(line);
	} finally{
		if(br != null){
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	regex = regex.substring(0, regex.length()-1);
	regex = regex + ").*";
	
	return Pattern.compile(regex);
}

//Takes an article as an XML fragment as input, output it only if relevant.
public static class WEMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String XMLString = value.toString();
			
		
			
			// Extracting stuff from title
			Matcher matcherTitle = titleRegex.matcher(XMLString);
			matcherTitle.find();
			String title = matcherTitle.group(1);
			
			String month = "";
			
			Matcher matcherMonth = monthRegex.matcher(title.toLowerCase());
			if(matcherMonth.find()){
				month = matcherMonth.group(1);
			}
			
			String year = "";
			
			Matcher matcherYear = yearRegex.matcher(title.toLowerCase());
			if(matcherYear.find()){
				year = matcherYear.group(1);
			}
			
			String country = "";
			
			Matcher matcherCountry = countryRegex.matcher(title.toLowerCase());
			if(matcherCountry.find()){
				country = matcherCountry.group(1);
			}
			
			
			System.out.println(title+": "+country+" "+month+" "+year);
			
			//Extracting stuff from infobox
			//TODO
			
			//Extracting stuff from article's body
			//TODO
			
			
	
		}
	}

	//Combine all XML format of articles from a category in a valid XML document
	public static class WEReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterator<Text> articles, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
				while(articles.hasNext()){
					output.collect(key, articles.next());	
				}	

		}

	}	
	
	//Allows us to generate a file for each category. Nicer output. (from https://sites.google.com/site/hadoopandhive/home/how-to-write-output-to-multiple-named-files-in-hadoop-using-multipletextoutputformat)
	static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
		
		@Override
        protected String generateFileNameForKeyValue(Text key, Text value,String name) {
                return key.toString()+".xml";
        }
        
        @Override
        protected Text generateActualKey(Text key, Text value) {
        	return new Text("");
        }
	}

	public static void main(String[] args) throws Exception {
		
		
		boolean deleteOutput = true;
		if(deleteOutput){
			FileSystem fs = FileSystem.get(new Configuration());
			fs.delete(new Path(args[1]), true);
		}
				
		JobConf conf = new JobConf(WikiExtract.class);
		conf.setJobName("Wikipedia filtering");
		//TODO check how many available on server
		conf.setNumMapTasks(88);
		conf.setNumReduceTasks(176);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(WEMap.class);
		conf.setReducerClass(WEReduce.class);

		conf.setInputFormat(XMLInputFormat.class);
		
		conf.setOutputFormat(MultiFileOutput.class);
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,  new Path(args[1]));

		JobClient.runJob(conf);

	}
}
