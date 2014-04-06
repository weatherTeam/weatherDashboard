
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
public class WikiFilter {

//A list of useful keywords that will define if an article is relevant or not (if the category contains one of those words).
public static String[] keywords = {"blizzard","cyclone","hurricane", "typhoon" ,"derecho","drought","nor'easter","storm","tornado","heat wave", "cold wave", "flood","weather event"};

//Regex to check if category contains one of the keywords
public static Pattern categoryIsRelevantRegex = createRegexFromKeywords();

//Regex to extract category from article
public static Pattern categoryRegex = Pattern.compile("\\[\\[Category:((\\w|\\s|\\d|')*)(\\|.*)?\\]\\]");

//Regex to extract title from article
public static Pattern findTitleRegex = Pattern.compile("<title>(.*)</title>");


//Create a regex of the form ".*(keyword1|keyword2|...|keywordn).*"
static Pattern createRegexFromKeywords(){
	String regex = ".*(";
	
	for(int i = 0; i < keywords.length - 1; i++){
		regex += keywords[i]+"|";
	}
	regex += keywords[keywords.length - 1];
	regex = regex + ").*";
	
	return Pattern.compile(regex);
}

//Takes an article as an XML fragment as input, output it only if relevant.
public static class WFMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String XMLString = value.toString();
			
		

			Matcher matcherCategory = categoryRegex.matcher(XMLString);
			
			//If an article is relevant, we output it.
		    while(matcherCategory.find()){
		    	String currentCategory = matcherCategory.group(1);
		    	currentCategory = currentCategory.trim();
		   
		    	Matcher matcherKeyword = categoryIsRelevantRegex.matcher(currentCategory.toLowerCase());
		    	if(matcherKeyword.find()){
		    		output.collect(new Text(matcherKeyword.group(1)), new Text(XMLString));
		    		break;
		    	}
		    	
			 }
			
			/* For debugging purposes
			  	if(!articleIsRelevant){
				Matcher matcherTitle = findTitleRegex.matcher(XMLString);
				matcherTitle.find();
				System.out.println(matcherTitle.group(1)+": "+categories);
			}*/	
			
			
		}
	}

	//Combine all XML format of articles from a category in a valid XML document
	public static class WFReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterator<Text> articles, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
				String XMLFile = "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.8/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.8/ http://www.mediawiki.org/xml/export-0.8.xsd\" version=\"0.8\" xml:lang=\"en\">  <siteinfo>    <sitename>Wikipedia</sitename>    <base>http://en.wikipedia.org/wiki/Main_Page</base>    <generator>MediaWiki 1.23wmf19</generator>    <case>first-letter</case>    <namespaces>      <namespace key=\"-2\" case=\"first-letter\">Media</namespace>      <namespace key=\"-1\" case=\"first-letter\">Special</namespace>      <namespace key=\"0\" case=\"first-letter\" />      <namespace key=\"1\" case=\"first-letter\">Talk</namespace>      <namespace key=\"2\" case=\"first-letter\">User</namespace>      <namespace key=\"3\" case=\"first-letter\">User talk</namespace>      <namespace key=\"4\" case=\"first-letter\">Wikipedia</namespace>      <namespace key=\"5\" case=\"first-letter\">Wikipedia talk</namespace>      <namespace key=\"6\" case=\"first-letter\">File</namespace>      <namespace key=\"7\" case=\"first-letter\">File talk</namespace>      <namespace key=\"8\" case=\"first-letter\">MediaWiki</namespace>      <namespace key=\"9\" case=\"first-letter\">MediaWiki talk</namespace>      <namespace key=\"10\" case=\"first-letter\">Template</namespace>      <namespace key=\"11\" case=\"first-letter\">Template talk</namespace>      <namespace key=\"12\" case=\"first-letter\">Help</namespace>      <namespace key=\"13\" case=\"first-letter\">Help talk</namespace>      <namespace key=\"14\" case=\"first-letter\">Category</namespace>      <namespace key=\"15\" case=\"first-letter\">Category talk</namespace>      <namespace key=\"100\" case=\"first-letter\">Portal</namespace>      <namespace key=\"101\" case=\"first-letter\">Portal talk</namespace>      <namespace key=\"108\" case=\"first-letter\">Book</namespace>      <namespace key=\"109\" case=\"first-letter\">Book talk</namespace>      <namespace key=\"118\" case=\"first-letter\">Draft</namespace>      <namespace key=\"119\" case=\"first-letter\">Draft talk</namespace>      <namespace key=\"446\" case=\"first-letter\">Education Program</namespace>      <namespace key=\"447\" case=\"first-letter\">Education Program talk</namespace>      <namespace key=\"710\" case=\"first-letter\">TimedText</namespace>      <namespace key=\"711\" case=\"first-letter\">TimedText talk</namespace>      <namespace key=\"828\" case=\"first-letter\">Module</namespace>      <namespace key=\"829\" case=\"first-letter\">Module talk</namespace>    </namespaces>  </siteinfo>";
		
				while(articles.hasNext()){
					XMLFile += articles.next().toString();	
				}
				
				XMLFile += "</mediawiki>";
				
				output.collect(key, new Text(XMLFile));

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
				
		JobConf conf = new JobConf(WikiFilter.class);
		conf.setJobName("Wikipedia filtering");
		//TODO check how many available on server
		conf.setNumMapTasks(88);
		conf.setNumReduceTasks(176);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(WFMap.class);
		conf.setReducerClass(WFReduce.class);

		conf.setInputFormat(XMLInputFormat.class);
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		
		conf.setOutputFormat(MultiFileOutput.class);
		
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,  new Path(args[1]));

		JobClient.runJob(conf);

	}
}
