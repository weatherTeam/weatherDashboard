package weather.wikipedia.infoExtraction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import weather.wikipedia.inputFormat.XMLInputFormat;

/*For a more detailed explanation see wiki: https://github.com/weatherTeam/weatherDashboard/wiki/Wikipedia*/
public class WikiExtract {

	// A list of useful keywords that will define if an article is relevant or
	// not (if the category contains one of those words).
	public static String[] months = { "january", "february", "march", "april",
			"may", "june", "july", "august", "september", "october",
			"november", "december", "winter", "spring", "summer", "autumn" };

	// Regex to check if category contains one of the keywords
	public static Pattern monthRegex = createRegexForMonths();

	// Regex for years
	public static Pattern yearRegex = Pattern.compile("\\d\\d\\d\\d");

	// Regex for countries/continents
	public static Pattern countryRegex = createRegexForCountries();

	// Regex to extract title from article
	public static Pattern titleRegex = Pattern.compile("<title>(.*)</title>");

	// Regex to extract infobox from article
	public static Pattern infoboxRegex = Pattern
			.compile("\\{\\{Infobox.*\n(.?\\|.*\n?)*\\}\\}");
	
	
	static String americanDate = "(january|february|march|april|may|june|july|august|september|october|november|december) (\\d?\\d), (\\d\\d\\d\\d)";
	static String standardDate = "(\\d?\\d) (january|february|march|april|may|june|july|august|september|october|november|december) (\\d\\d\\d\\d)";
	
	static String filename = new String();
	
	// Create a regex of the form "(keyword1|keyword2|...|keywordn)"
	static Pattern createRegexForMonths() {
		String regex = "(";

		for (int i = 0; i < months.length - 1; i++) {
			regex += months[i] + "|";
		}
		regex += months[months.length - 1];
		regex = regex + ")";

		return Pattern.compile(regex);
	}

	// Create a regex to extract country, from a predefined list
	static Pattern createRegexForCountries() {
		String regex = "((north|west|east|south)(ern)? ?)*(";

		String countryFile = "/wikipedia/demonyms.txt";
		BufferedReader br = null;
		String line = "";

		try {
			InputStream test = WikiExtract.class.getResourceAsStream(countryFile);
			InputStreamReader in = new InputStreamReader(test);
			br = new BufferedReader(in);
			while ((line = br.readLine()) != null) {
				line = line.toLowerCase();
				String countryAndDemonym[] = line.split("\t");
				if (countryAndDemonym.length > 1) {
					regex += countryAndDemonym[0] + "|" + countryAndDemonym[1]
							+ "|";
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(line);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		regex = regex.substring(0, regex.length() - 1);
		regex = regex + ")";

		return Pattern.compile(regex);
	}

	// Takes an article as an XML fragment as input, output it only if relevant.
	public static class WEMap extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, WeatherEvent> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, WeatherEvent> output, Reporter reporter)
				throws IOException {
			
			String XMLString = value.toString();
			
			if (isSport(XMLString)){
				return;
			}
			
			//Info we have to find:
			String title = "";
			String category = "";
			String startDate = "";
			String endDate = "";
			String location = "";
			
			
			

			// Extracting stuff from title
			Matcher matcherTitle = titleRegex.matcher(XMLString);
			matcherTitle.find();
			title = matcherTitle.group(1);
			
			if(title.startsWith("Category:") || title.startsWith("Template:") || title.startsWith("File:") || title.startsWith("Book:") || title.startsWith("Wikipedia:")){
				return;
			}

			String month = "";

			Matcher matcherMonth = monthRegex.matcher(title.toLowerCase());
			if (matcherMonth.find()) {
				month = matcherMonth.group(0);
			}

			String year = "";

			Matcher matcherYear = yearRegex.matcher(title.toLowerCase());
			if (matcherYear.find()) {
				year = matcherYear.group(0);
			}
			
			//TODO do it with full date, not only year
			if (!year.equals("") && (Integer.parseInt(year) < 1900 || Integer.parseInt(year) > 2014)){
				return;
			}

			String country = "";

			Matcher matcherCountry = countryRegex.matcher(title.toLowerCase());
			if (matcherCountry.find()) {
				country = matcherCountry.group(0);
			}
			
			if(country != ""){
				location = country;
			}

			//System.out.println(title + ": " + country + " " + month + " "+ year);

			// Extracting stuff from infobox (maybe there is no need to extract
			// infobox, and extract info directly)
			// TODO

			String infobox = "";

			Matcher matcherInfobox = infoboxRegex.matcher(XMLString);
			if (matcherInfobox.find()) {
				infobox = matcherInfobox.group(0);
			}

			if (!infobox.equals("")) {
				// System.out.println(title+": \n"+infobox);

				infobox = infobox.replaceAll("&amp;nbsp;", " ");
				
				Pattern startDatePattern = Pattern.compile("formed.*=.*(("+americanDate+")|("+standardDate+")).*\n");
				String start = "";

				Matcher matcherStart = startDatePattern.matcher(infobox.toLowerCase());
				if (matcherStart.find()) {
					start = matcherStart.group(1);
				}
				
				if(start != ""){
					startDate = start;
				}

				Pattern endDatePattern = Pattern.compile("dissipated.*=.*(("+americanDate+")|("+standardDate+")).*\n");
				String end = "";				

				Matcher matcherEnd = endDatePattern.matcher(infobox.toLowerCase());
				if (matcherEnd.find()) {
					end = matcherEnd.group(1);
				}
				
				if(end != ""){
					endDate = end;
				}

				Pattern areasReg = Pattern.compile("areas.*=(.*)\n?");
				String areas = "";

				Matcher matcherAreas = areasReg.matcher(infobox.toLowerCase());
				if (matcherAreas.find()) {
					areas = matcherAreas.group(1);
				}

				//System.out.println(title + ": " + areas + "|" + start + " - "+ end);
				
				if(areas != ""){
					areas = areas.replaceAll("&gt;(br)?|&lt;(br)?|([^A-Za-z\\s])|flag(icon)?", "");
					location = location+" "+areas;
				}
			}

			// output.collect(new Text(), new Text(infobox));

			// Extracting stuff from article's body
			// TODO
			
			if(startDate == ""){
				
				if (month != "" && year != ""){
					
					//Seasons are not clearly defined, eg: http://en.wikipedia.org/wiki/Winter_storms_of_2009–10_in_East_Asia
					if (month.equals("winter")) { //TODO: Does not work in southern hemisphere
						startDate = "1/12/"+year;
						endDate = "31/03/"+(Integer.parseInt(year)+1);
					} else if (month.equals("summer")){
						startDate = "1/06/"+year;
						endDate = "31/09/"+year;
					} else if (month.equals("spring")){
						startDate = "1/03/"+year;
						endDate = "31/06/"+year;
					} else if (month.equals("autumn")){
						startDate = "1/09/"+year;
						endDate = "31/12/"+year;
					} else {
						startDate = "1/"+monthToNum(month)+"/"+ year;
						endDate = getLastDayOfMonth(month, year)+"/"+monthToNum(month)+"/"+ year;
					}
				} else if (year != ""){
					startDate = "1/01/"+year;
					endDate = "31/12/"+year;
				}
			}
			
			if (endDate == "" && startDate != ""){
				endDate = startDate;
			}
			
			startDate = formatDate(startDate);
			endDate = formatDate(endDate);
			
			String[] tmp = filename.split("/");
			category = tmp[tmp.length-1];
			category = category.replaceAll("\\.xml|\\d", "");
			//category = category.replaceAll("\\.xml", "");
			
			
			if(startDate.equals("")){
				startDate="-";
			}
			
			if(endDate.equals("")){
				endDate="-";
			}
			
			if(location.equals("")){
				location="-";
			}
			
		
			if(!startDate.equals("-") || !location.equals("-")){	
				output.collect(NullWritable.get(), new WeatherEvent(title, category, startDate, endDate, location));
			}
		}
		
		private int getLastDayOfMonth(String month, String year){
			if(month.equals( "january" )|| month.equals( "march") || month.equals( "may") || month.equals( "july") || month.equals( "august") || month.equals( "october") || month.equals( "december")){
				return 31;
			}
			
			if (month.equals("april") || month.equals("june") || month.equals("september") || month.equals("november")){
				return 30;
			}
			
			if (month.equals("february")){ 
				int yearInt = Integer.parseInt(year);
				if (yearInt != 2000 && yearInt % 4 == 0){
					return 29;
				}
				return 28;
			} 
			
			return 1;
			
		}
		
		private String formatDate(String date){
			
			date = date.trim();
			
			if (date == ""){
				return date;
			} 
			
			String dateFormat = "\\d?\\d/\\d\\d/\\d\\d\\d\\d";
			
			date = date.replaceAll("nbsp;", " ");
			date = date.replaceAll("&gt;(br)?|&lt;(br)?|&amp;", "");
						
			Pattern americanFormat = Pattern.compile(americanDate);
					
			if(date.matches(dateFormat)){
				return date;
			}
			
			Matcher americanDate = americanFormat.matcher(date.toLowerCase());
			if (americanDate.find()) {
				return americanDate.group(2)+"/"+monthToNum(americanDate.group(1))+"/"+americanDate.group(3);
			}
			
			Pattern standardFormat = Pattern.compile(standardDate);
			
			Matcher standardDate = standardFormat.matcher(date.toLowerCase());
			if (standardDate.find()) {
				return standardDate.group(1)+"/"+monthToNum(standardDate.group(2))+"/"+standardDate.group(3);
			}
			
			System.out.println(date);
			return "XXXX: "+date;
			
		}
		
		private String monthToNum(String month){
			if(month.equals("january")){
				return "01";
			}
			if(month.equals("february")){
				return "02";
			}
			if(month.equals("march")){
				return "03";
			}
			if(month.equals("april")){
				return "04";
			}
			if(month.equals("may")){
				return "05";
			}
			if(month.equals("june")){
				return "06";
			}
			if(month.equals("july")){
				return "07";
			}
			if(month.equals("august")){
				return "08";
			}
			if(month.equals("september")){
				return "09";
			}
			if(month.equals("october")){
				return "10";
			}
			if(month.equals("november")){
				return "11";
			}
			if(month.equals("december")){
				return "12";
			}
			
			return month;
			
		}
		
		//TODO: Move to Filter file, eventually
		private boolean isSport(String article){
			
			Pattern categoryRegex = Pattern.compile("\\[\\[category:((\\w|\\s|\\d|')*)(\\|.*)?\\]\\]");
			Matcher matcherCategory = categoryRegex.matcher(article.toLowerCase());

			// If an article is relevant, we output it.
			while (matcherCategory.find()) {
				String currentCategory = matcherCategory.group(1);
				currentCategory = currentCategory.trim();
				
				Matcher matcherSport= falsePositiveRegex.matcher(currentCategory.toLowerCase());
				if(matcherSport.find()){
					return true;
				}
				
			}
			
			return false;
		}
		
		static Pattern falsePositiveRegex = createRegexForFalsePositives();
		
		static Pattern createRegexForFalsePositives() {
			
			String[] falsePositives = {"basketball", "rugby", "baseball", "football", "hockey", "soccer", "comics", "video game", "song", "album", "book", "entertainment", "film", "movie"};
			
			String regex = ".*(";

			for (int i = 0; i < falsePositives.length - 1; i++) {
				regex += falsePositives[i] + "|";
			}
			regex += falsePositives[falsePositives.length - 1];
			regex = regex + ").*";

			return Pattern.compile(regex);
		}
		
		public void configure(JobConf job)
		{
		   filename = job.get("map.input.file");
		}
		
	}

	
	public static class WEReduce extends MapReduceBase implements
			Reducer<NullWritable, WeatherEvent, NullWritable, WeatherEvent> {

		@Override
		public void reduce(NullWritable key, Iterator<WeatherEvent> values,
				OutputCollector<NullWritable, WeatherEvent> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				// TODO Add result to a database or something (or store
				// everything in a csv, i don't know)
				output.collect(NullWritable.get(), values.next());
			}

		}

	}

	// Allows us to generate a file for each category. Nicer output. (from
	// https://sites.google.com/site/hadoopandhive/home/how-to-write-output-to-multiple-named-files-in-hadoop-using-multipletextoutputformat)
	/*static class MultiFileOutputWE extends MultipleTextOutputFormat<NullWritable, WeatherEvent> {

		@Override
		protected String generateFileNameForKeyValue(NullWritable key, WeatherEvent value,
				String name) {
			return value.category.toString() + ".tsv";
		}
		
	}*/

	public static void main(String[] args) throws Exception {

		boolean deleteOutput = true;
		if (deleteOutput) {
			FileSystem fs = FileSystem.get(new Configuration());
			fs.delete(new Path(args[1]), true);
		}

		JobConf conf = new JobConf(WikiExtract.class);
		conf.setJobName("Wikipedia Extraction");
		//conf.setNumMapTasks(88);
		conf.setNumReduceTasks(0);

		conf.setMapOutputKeyClass(NullWritable.class);
		conf.setMapOutputValueClass(WeatherEvent.class);

		conf.setMapperClass(WEMap.class);
		//conf.setReducerClass(WEReduce.class);

		conf.setInputFormat(XMLInputFormat.class);
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setOutputFormat(MultiFileOutputWE.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}
}
