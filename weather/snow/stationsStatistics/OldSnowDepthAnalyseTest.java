package weather.snow.stationsStatistics;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;


/**
 * How it works
 * <p/>
 * IDEA
 * The method looks at each user. By looking at his friends we suppose that it exists a triangular relation. So we
 * emit the supposition
 * that A-B-C are in a triangular relation. If indeed this relation exists, then by looking at the friends of B,
 * we will again suppose that
 * this relation exists. And the same for C. If the relation exists, then we will emit the supposition 3 times. In
 * the other case it will be lower.
 * This is how we find triangular relationship.
 * <p/>
 * MAPPER
 * For each userID, it emit a a key = (userID, f1, f2), where f1, f2 are 2 friends of userID. The 3 are ordered by
 * their ID. The value emitted it 1.
 * <p/>
 * REDUCER 1
 * We receive key (userID, f1, f2) and value {1, 1, 1} or {1}. We check if there the relation exists 3 times. If it
 * is the case, then there is
 * a triangular relation, then we emit a key INTERMED_KEY and a value (1)
 * <p/>
 * REDUCER 2
 * We receive (INTERMED_KEY, {1,1,1,1,1,1,1,1, ... , 1}). By summing all this,
 * we find how many triangular connections exists.
 *
 * @author Jonathan
 */


public class OldSnowDepthAnalyseTest {


	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
		                Reporter reporter) throws IOException {


			String input = value.toString();


			String snowDepthString = "";
			String year = "";
			String month = "";
			String day = "";
			String time = "";

			final int yearPosition = 13; //-1 because of java substring function: begin index is excluded,
			// last is included
			final int yearPositionEnd = 17;
			final int monthPosition = 17; //-1 because of java substring function: begin index is excluded,
			// last is included
			final int monthPositionEnd = 19;
			final int dayPosition = 19; //-1 because of java substring function: begin index is excluded,
			// last is included
			final int dayPositionEnd = 21;
			final int timePosition = 21; //-1 because of java substring function: begin index is excluded,
			// last is included
			final int timePositionEnd = 23;

			final int snowDepthPosition = 146 - 1;
			final int snowDepthPositionEnd = 147;

			if (input.length() == 147) {
				year = input.substring(yearPosition, yearPositionEnd);
				month = input.substring(monthPosition, monthPositionEnd);
				day = input.substring(dayPosition, dayPositionEnd);
				time = input.substring(timePosition, timePositionEnd);
				snowDepthString = input.substring(snowDepthPosition, snowDepthPositionEnd);
			}

			if (snowDepthString.equals("**") == false && snowDepthString.length() > 0) {
				System.out.println("" + year + " " + month + " " + day + " " + time + "h00 " + snowDepthString);
				System.out.println();
				int snowDepth = Integer.parseInt(snowDepthString);

				//output: key: YEAR-MONTH
				//output: value: snow depth
				output.collect(new Text(year + "-" + month), new IntWritable(snowDepth));

			}


		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, FloatWritable> output,
		                   Reporter reporter) throws IOException {

            /*
             * Each key-value pair is of kind: "year-month" - {43, 50, 60, 30, 20, 0}. Values are all measured depth
             * in the time period given by the key
             */

			String keyString = key.toString();
			String[] date = keyString.split("-");
			int month = Integer.parseInt(date[1]);

			Month m = Month.valueOf(Month.values()[month].toString());


			float result = 0.0f;

			while (values.hasNext()) {
				int val = values.next().get();

				result += (float) val / m.numDays();

			}

			output.collect(key, new FloatWritable(result));


		}


	}


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		//Path p = new Path(args[1]);
		Path tempPath = new Path(args[1]);

		JobConf conf1 = new JobConf(OldSnowDepthAnalyseTest.class);
		conf1.setJobName("hw2");

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(FloatWritable.class);

		conf1.setMapperClass(Map.class);
		conf1.setReducerClass(Reduce.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		conf1.setNumMapTasks(88);
		conf1.setNumReduceTasks(88);

		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf1, tempPath);


		JobClient.runJob(conf1);

//		JobControl control = new JobControl("hw2-controller");
//		
//		org.apache.hadoop.mapred.jobcontrol.Job j1 = new org.apache.hadoop.mapred.jobcontrol.Job(conf1);
//		org.apache.hadoop.mapred.jobcontrol.Job j2 = new org.apache.hadoop.mapred.jobcontrol.Job(conf2);
//
//		
//		control.addJob(j1);
//		control.addJob(j2);
//		
//		j2.addDependingJob(j1);
//
//		control.run();


	}


}


//doesn't work. Don't get how these custom enums works in java
enum Month {
	empty(0),
	january(31),
	february(28),
	march(31),
	april(30),
	may(31),
	june(30),
	july(31),
	august(31),
	september(30),
	october(31),
	november(30),
	december(31);

	private int month;

	private void value(int v) {
		month = v;
	}

	Month(int m) {
		this.month = m;
	}

	public int numDays() {
		return month;
	}

	public Month getMonth(int i) {
		if (i > 0 && i <= 12) {
			return Month.values()[i];
		}
		return null;
	}

}