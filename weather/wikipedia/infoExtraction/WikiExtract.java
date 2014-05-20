package weather.wikipedia.infoExtraction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import weather.wikipedia.inputFormat.XMLInputFormat;

/*For a more detailed explanation see wiki: https://github.com/weatherTeam/weatherDashboard/wiki/Wikipedia*/
public class WikiExtract {

	public static final int PERIOD_START = 1900;
	public static final int PERIOD_END = Calendar.getInstance().get(Calendar.YEAR);

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

	static String americanDate = "(january|february|march|april|may|june|july|august|september|october|november|december) (\\d?\\d), (\\d\\d\\d\\d)";
	static String standardDate = "(\\d?\\d) (january|february|march|april|may|june|july|august|september|october|november|december) (\\d\\d\\d\\d)";
	static String numericalDate = "(\\d\\d\\d\\d)\\W([01]\\d)\\W([0123]\\d)";
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

	// Create a regex to extract countries and corresponding demonyms, from a
	// predefined list (from Stanford NLP)
	static Pattern createRegexForCountries() {
		String regex = "((north|west|east|south)(ern)? ?)*(";

		String countryFile = "/wikipedia/demonyms.txt";
		BufferedReader br = null;
		String line = "";

		try {
			// Usual Java stuff to read file from jar
			InputStream test = WikiExtract.class
					.getResourceAsStream(countryFile);
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
			// Close file
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

	/**
	 * Extract information from each given Wikipedia article
	 */
	public static class WEMap extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, WeatherEvent> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, WeatherEvent> output,
				Reporter reporter) throws IOException {

			String XMLString = value.toString();

			// Info we have to find:
			String title = "";
			String category = "";
			String startDate = "";
			String endDate = "";
			String location = "";

			// Instead of extracting again the category, we get it from the
			// filename. (Not sure which way is better)
			String[] tmp = filename.split("/");
			category = tmp[tmp.length - 1];
			category = category.replaceAll("\\.xml|\\d", "");

			// Actually, we don't care about floods, but I'm too lazy to
			// refilter
			// the whole dataset.
			if (category.equals("flood")) {
				return;
			}

			// Extracting stuff from title
			Matcher matcherTitle = titleRegex.matcher(XMLString);
			matcherTitle.find();
			title = matcherTitle.group(1);

			// TODO: Move it to WikiFilter eventually
			if (title.startsWith("Category:") || title.startsWith("Template:")
					|| title.startsWith("File:") || title.startsWith("Book:")
					|| title.startsWith("Wikipedia:")
					|| title.startsWith("Portal:")) {
				return;
			}

			XMLString = XMLString.toLowerCase();

			// Will be irrelevant once we have document categorisation working
			if (isFalsePositive(XMLString)) {
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

			String country = "";

			Matcher matcherCountry = countryRegex.matcher(title.toLowerCase());
			if (matcherCountry.find()) {
				country = matcherCountry.group(0);
			}

			if (!country.equals("")) {
				location = country;
			}

			// Extracting stuff from infobox

			String infobox = "";

			// Starting point of the infobox
			int index = XMLString.indexOf("{{infobox");

			// Brackets counter to stop when balanced
			int numBrackets = 0;
			String infoboxBuffer = "";

			if (index != -1) {
				do {
					char currentChar = XMLString.charAt(index);
					infoboxBuffer += currentChar;

					if (currentChar == '{') {
						numBrackets++;
					} else if (currentChar == '}') {
						numBrackets--;
					}
					index++;
				} while (numBrackets != 0 && index < XMLString.length());

				if (numBrackets == 0) {
					infobox = infoboxBuffer;
				}
			}

			if (!infobox.equals("")) {

				// Replace some symbols
				infobox = infobox.replaceAll("&amp;nbsp;", " ");

				// Regex of the formed "(date|formed).*=.*(event date).*"
				Pattern startDatePattern = Pattern
						.compile("(?:date|formed).*=.*?((" + americanDate
								+ ")|(" + standardDate + ")|(" + numericalDate
								+ ")).*\n");
				String start = "";

				Matcher matcherStart = startDatePattern.matcher(infobox);
				if (matcherStart.find()) {
					start = matcherStart.group(1);
				}

				if (!start.equals("")) {
					startDate = start;
				}

				// Regex of the formed "dissipated.*=.*(event date).*"
				Pattern endDatePattern = Pattern.compile("dissipated.*=.*?(("
						+ americanDate + ")|(" + standardDate + ")|("
						+ numericalDate + ")).*\n");
				String end = "";

				Matcher matcherEnd = endDatePattern.matcher(infobox);
				if (matcherEnd.find()) {
					end = matcherEnd.group(1);
				}

				if (!end.equals("")) {
					endDate = end;
				}

				Pattern areasReg = Pattern.compile("areas?.*=(.*)\n?");
				String areas = "";

				Matcher matcherAreas = areasReg.matcher(infobox);
				if (matcherAreas.find()) {
					areas = matcherAreas.group(1);
				}

				// System.out.println(title + ": " + areas + "|" + start +
				// " - "+ end);

				if (!areas.equals("")) {
					areas = areas.replaceAll(
							"&gt;(br)?|&lt;(br)?|([^A-Za-z\\s])|flag(icon)?",
							"");
					areas = areas.trim();
					location = location + " " + areas;
				}

			}

			// Extracting stuff from article's body

			// Last resort: If we do not have any location information from
			// title and infobox, we try to extract it from text
			if (location.equals("")) {
				location = "";
				Matcher matcherLocation = countryRegex.matcher(XMLString);
				while (matcherLocation.find()) {
					String place = matcherLocation.group(0);
					if (!location.contains(place)) {
						location += " " + place;
					}
				}

			}

			if (startDate.equals("")) {

				if (!month.equals("") && !year.equals("")) {

					// Seasons are not clearly defined, eg:
					// http://en.wikipedia.org/wiki/Winter_storms_of_2009–10_in_East_Asia
					if (month.equals("winter")) { // TODO: Does not work in
													// southern hemisphere
						startDate = "1/12/" + year;
						endDate = "31/03/" + (Integer.parseInt(year) + 1);
					} else if (month.equals("summer")) {
						startDate = "1/06/" + year;
						endDate = "31/09/" + year;
					} else if (month.equals("spring")) {
						startDate = "1/03/" + year;
						endDate = "31/06/" + year;
					} else if (month.equals("autumn")) {
						startDate = "1/09/" + year;
						endDate = "31/12/" + year;
					} else {
						// If we have month and year
						startDate = "1/" + monthToNum(month) + "/" + year;
						endDate = getLastDayOfMonth(month, year) + "/"
								+ monthToNum(month) + "/" + year;
					}
				} else if (!year.equals("")) {
					// If we only have the year
					startDate = "1/01/" + year;
					endDate = "31/12/" + year;
				}
			}

			// If we have no end date, we suppose it is the same as the start
			// date
			if (endDate.equals("") && !startDate.equals("")) {
				endDate = startDate;
			}

			// Conver to DD/MM/YYYY date format
			startDate = formatDate(startDate);
			endDate = formatDate(endDate);

			// Event is beween 1900 and now
			if (!isInTimeRange(startDate, endDate)) {
				return;
			}

			if (startDate.equals("")) {
				startDate = "-";
			}

			if (endDate.equals("")) {
				endDate = "-";
			}

			location = location.trim();
			if (location.equals("")) {
				location = "-";
			}

			// We output only if we managed to extract some information
			if (!startDate.equals("-") || !location.equals("-")) {
				output.collect(NullWritable.get(), new WeatherEvent(title,
						category, startDate, endDate, location));
			}
		}

		private int getLastDayOfMonth(String month, String year) {
			if (month.equals("january") || month.equals("march")
					|| month.equals("may") || month.equals("july")
					|| month.equals("august") || month.equals("october")
					|| month.equals("december")) {
				return 31;
			}

			if (month.equals("april") || month.equals("june")
					|| month.equals("september") || month.equals("november")) {
				return 30;
			}

			if (month.equals("february")) {
				int yearInt = Integer.parseInt(year);
				// between 1900 and now, only 2000 is weird
				if (yearInt != 2000 && yearInt % 4 == 0) {
					return 29;
				}
				return 28;
			}

			return 1;

		}

		/**
		 * 
		 * @param date
		 *            : a string representing a date
		 * @return a date in DD/MM/YYYY format
		 */
		private String formatDate(String date) {

			date = date.trim();

			if (date.equals("")) {
				return date;
			}

			String dateFormat = "\\d?\\d/\\d\\d/\\d\\d\\d\\d";

			date = date.replaceAll("nbsp;", " ");
			date = date.replaceAll("&gt;(br)?|&lt;(br)?|&amp;", "");

			// Already formated
			if (date.matches(dateFormat)) {
				//We make sure that days are in the format 0\d and not \d
				try{
				return ('0'+date).substring(date.length() - 9);
				} catch(Exception e){
					System.out.println(date);
				}
			}

			// For dates in American format eg. April 30, 1991
			Pattern americanFormat = Pattern.compile(americanDate);

			Matcher americanDate = americanFormat.matcher(date);
			if (americanDate.find()) {
				
				String day = "0"+americanDate.group(2);
				day = day.substring(day.length()-2);
				
				return day + "/"
						+ monthToNum(americanDate.group(1)) + "/"
						+ americanDate.group(3);
			}

			// For date in "normal" format eg. 30 April 1991
			Pattern standardFormat = Pattern.compile(standardDate);

			Matcher standardDate = standardFormat.matcher(date);
			if (standardDate.find()) {
				
				String day = "0"+standardDate.group(1);
				day = day.substring(day.length()-2);
				
				return day + "/"
						+ monthToNum(standardDate.group(2)) + "/"
						+ standardDate.group(3);
			}

			// For dates in YYYY/MM/DD format
			Pattern numericalFormat = Pattern.compile(numericalDate);

			Matcher numericalDate = numericalFormat.matcher(date);
			if (numericalDate.find()) {
				
				String day = "0"+numericalDate.group(3);
				day = day.substring(day.length()-2);
				
				return day + "/" + numericalDate.group(2)
						+ "/" + numericalDate.group(1);
			}

			System.out.println(date);

			// For debugging purpose
			return "XXXX: " + date;

		}

		// Did not find any function in the API for that, maybe I should search
		// more
		private String monthToNum(String month) {
			if (month.equals("january")) {
				return "01";
			}
			if (month.equals("february")) {
				return "02";
			}
			if (month.equals("march")) {
				return "03";
			}
			if (month.equals("april")) {
				return "04";
			}
			if (month.equals("may")) {
				return "05";
			}
			if (month.equals("june")) {
				return "06";
			}
			if (month.equals("july")) {
				return "07";
			}
			if (month.equals("august")) {
				return "08";
			}
			if (month.equals("september")) {
				return "09";
			}
			if (month.equals("october")) {
				return "10";
			}
			if (month.equals("november")) {
				return "11";
			}
			if (month.equals("december")) {
				return "12";
			}

			return month;

		}

		// TODO: Move to Filter class, eventually
		/**
		 * Remove false positives i.e. articles whose categories contain one of
		 * the keywords but who are not relevant to weather (eg. sports team
		 * named Hurricanes, Blizzard Entertainment, etc.)
		 */
		private boolean isFalsePositive(String article) {

			Pattern categoryRegex = Pattern
					.compile("\\[\\[category:((\\w|\\s|\\d|')*)(\\|.*)?\\]\\]");
			Matcher matcherCategory = categoryRegex.matcher(article);

			// If an article is relevant, we output it.
			while (matcherCategory.find()) {
				String currentCategory = matcherCategory.group(1);
				currentCategory = currentCategory.trim();

				Matcher matcherSport = falsePositiveRegex
						.matcher(currentCategory);
				if (matcherSport.find()) {
					return true;
				}

			}

			return false;
		}

		static Pattern falsePositiveRegex = createRegexForFalsePositives();

		static Pattern createRegexForFalsePositives() {

			String[] falsePositives = { "basketball", "rugby", "baseball",
					"football", "softball", "hockey", "soccer", "comics",
					"video game", "song", "album", "book", "entertainment",
					"film", "movie" };

			String regex = ".*(";

			for (int i = 0; i < falsePositives.length - 1; i++) {
				regex += falsePositives[i] + "|";
			}
			regex += falsePositives[falsePositives.length - 1];
			regex = regex + ").*";

			return Pattern.compile(regex);
		}

		/**
		 * @return true if event takes place between {@link #PERIOD_START} and
		 *         {@link #PERIOD_END}, false otherwise
		 */
		boolean isInTimeRange(String startDate, String endDate) {
			
			//If we have no time info we (don't) dismiss the event
			if (startDate.equals("")) {
				//return false;
				return true;
			}

			int startYear = Integer.parseInt(startDate.split("/")[2]);
			int endYear = Integer.parseInt(endDate.split("/")[2]);

			if (endYear < PERIOD_START) {
				return false;
			}

			if (startYear > PERIOD_END) {
				return false;
			}

			return true;

		}

		//Allows us to access the name of the input file
		public void configure(JobConf job) {
			filename = job.get("map.input.file");
		}

	}

	public static void main(String[] args) throws Exception {

		boolean deleteOutput = true;
		if (deleteOutput) {
			FileSystem fs = FileSystem.get(new Configuration());
			fs.delete(new Path(args[1]), true);
		}

		JobConf conf = new JobConf(WikiExtract.class);
		conf.setJobName("Wikipedia Extraction");
		conf.setNumMapTasks(88);
		
		//We don't need a reducer (map does all the work)
		conf.setNumReduceTasks(0);

		conf.setMapOutputKeyClass(NullWritable.class);
		conf.setMapOutputValueClass(WeatherEvent.class);

		conf.setMapperClass(WEMap.class);
		
		//XML config
		conf.setInputFormat(XMLInputFormat.class);
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.setOutputFormat(TextOutputFormat.class);
		// conf.setOutputFormat(MultiFileOutputWE.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}
}
