
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class WikiFilter {
	
public static String[] keywords = {"blizzard","cyclone","hurricane", "typhoon" ,"derecho","drought","nor'easter","storm","tornado","wave", "weather event"};
public static Pattern categoryIsRelevantRegex = createRegexFromKeywords();
public static Pattern categoryRegex = Pattern.compile("\\[\\[Category:((\\w|\\s|\\d|')*)(\\|.*)?\\]\\]");	
public static Pattern findTitleRegex = Pattern.compile("<title>(.*)</title>");

static Pattern createRegexFromKeywords(){
	String regex = ".*(";
	
	for(int i = 0; i < keywords.length - 1; i++){
		regex += keywords[i]+"|";
	}
	regex += keywords[keywords.length - 1];
	regex = regex + ").*";
	
	return Pattern.compile(regex);
}

static String encodeCategory(String category){
	category = category.replace(' ', '0');
	category = category.replace('\'', '0');
	
	return category;
}


public static class WFMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String XMLString = value.toString();
			
			String articleCategory = "";
			
			boolean articleIsRelevant = false;

			Matcher matcherCategory = categoryRegex.matcher(XMLString);
		    while(matcherCategory.find()&&!articleIsRelevant){
		    	String currentCategory = matcherCategory.group(1);
		    	currentCategory = currentCategory.trim();
		   
		    	Matcher matcherKeyword = categoryIsRelevantRegex.matcher(currentCategory.toLowerCase());
		    	articleIsRelevant = matcherKeyword.find();
		    	if(articleIsRelevant){
		    		articleCategory = matcherKeyword.group(1);
		    	}
		    	
			 }
		    
		    if(!articleCategory.equals("")){
		    	output.collect(new Text(articleCategory), new Text(XMLString));
		    }
			
			/*if(!articleIsRelevant){
				Matcher matcherTitle = findTitleRegex.matcher(XMLString);
				matcherTitle.find();
				System.out.println(matcherTitle.group(1)+": "+categories);
			}*/	
			
			
		}
	}


	public static class WFReduce extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> {
		
		MultipleOutputs mos = null;
		
		public void configure(JobConf job) {
	        mos = new MultipleOutputs(job);
	    }

		@Override
		public void reduce(Text key, Iterator<Text> articles, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
			
				String XMLFile = "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.8/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.8/ http://www.mediawiki.org/xml/export-0.8.xsd\" version=\"0.8\" xml:lang=\"en\">  <siteinfo>    <sitename>Wikipedia</sitename>    <base>http://en.wikipedia.org/wiki/Main_Page</base>    <generator>MediaWiki 1.23wmf19</generator>    <case>first-letter</case>    <namespaces>      <namespace key=\"-2\" case=\"first-letter\">Media</namespace>      <namespace key=\"-1\" case=\"first-letter\">Special</namespace>      <namespace key=\"0\" case=\"first-letter\" />      <namespace key=\"1\" case=\"first-letter\">Talk</namespace>      <namespace key=\"2\" case=\"first-letter\">User</namespace>      <namespace key=\"3\" case=\"first-letter\">User talk</namespace>      <namespace key=\"4\" case=\"first-letter\">Wikipedia</namespace>      <namespace key=\"5\" case=\"first-letter\">Wikipedia talk</namespace>      <namespace key=\"6\" case=\"first-letter\">File</namespace>      <namespace key=\"7\" case=\"first-letter\">File talk</namespace>      <namespace key=\"8\" case=\"first-letter\">MediaWiki</namespace>      <namespace key=\"9\" case=\"first-letter\">MediaWiki talk</namespace>      <namespace key=\"10\" case=\"first-letter\">Template</namespace>      <namespace key=\"11\" case=\"first-letter\">Template talk</namespace>      <namespace key=\"12\" case=\"first-letter\">Help</namespace>      <namespace key=\"13\" case=\"first-letter\">Help talk</namespace>      <namespace key=\"14\" case=\"first-letter\">Category</namespace>      <namespace key=\"15\" case=\"first-letter\">Category talk</namespace>      <namespace key=\"100\" case=\"first-letter\">Portal</namespace>      <namespace key=\"101\" case=\"first-letter\">Portal talk</namespace>      <namespace key=\"108\" case=\"first-letter\">Book</namespace>      <namespace key=\"109\" case=\"first-letter\">Book talk</namespace>      <namespace key=\"118\" case=\"first-letter\">Draft</namespace>      <namespace key=\"119\" case=\"first-letter\">Draft talk</namespace>      <namespace key=\"446\" case=\"first-letter\">Education Program</namespace>      <namespace key=\"447\" case=\"first-letter\">Education Program talk</namespace>      <namespace key=\"710\" case=\"first-letter\">TimedText</namespace>      <namespace key=\"711\" case=\"first-letter\">TimedText talk</namespace>      <namespace key=\"828\" case=\"first-letter\">Module</namespace>      <namespace key=\"829\" case=\"first-letter\">Module talk</namespace>    </namespaces>  </siteinfo>";
		
				while(articles.hasNext()){
					XMLFile += articles.next().toString();	
				}
				
				XMLFile += "</mediawiki>";
				
				//mos.getCollector(encodeCategory(key.toString()), reporter).collect(NullWritable.get(), new Text(XMLFile));
				output.collect(NullWritable.get(), new Text(XMLFile));

		}

	}	
	
	public static class WFCombine extends MapReduceBase implements Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

		@Override
		public void reduce(NullWritable key, Iterator<IntWritable> values, OutputCollector<NullWritable, IntWritable> output, Reporter reporter) throws IOException {
			

		}

	}

	public static void main(String[] args) throws Exception {
		
		
		boolean local = true;
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path("output"), true);
				
		JobConf conf = new JobConf(WikiFilter.class);
		conf.setJobName("Wikipedia filtering");
		conf.setNumMapTasks(88);
		conf.setNumReduceTasks(176);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(WFMap.class);
		conf.setReducerClass(WFReduce.class);

		conf.setInputFormat(XMLInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		
		for(String key: keywords){
			key = encodeCategory(key);
			MultipleOutputs.addNamedOutput(conf, key, org.apache.hadoop.mapred.TextOutputFormat.class, NullWritable.class, Text.class);
		}
		
		
		

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		
		if(!local){
			FileOutputFormat.setOutputPath(conf, new Path("/std022/hw2/tmp"));
		} else {
			FileOutputFormat.setOutputPath(conf,  new Path(args[1]));
		}

		JobClient.runJob(conf);
		
		
		/*
		// We delete the tmp folder
		FileSystem fs = FileSystem.get(new Configuration());
		if(local){
			fs.delete(new Path("tmp"), true);
		} else {
			fs.delete(new Path("/std022/hw2/tmp"), true);
		}
		*/

	}
}
