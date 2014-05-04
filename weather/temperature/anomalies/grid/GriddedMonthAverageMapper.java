package weather.temperature.anomalies.grid;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class GriddedMonthAverageMapper extends MapReduceBase implements
		Mapper<Text, Text, Text, Text> {

	private static double xStep;
	private static double yStep;
	
	public void configure(JobConf job) {
		xStep = Double.parseDouble(job.get("xStep"));
		yStep = Double.parseDouble(job.get("yStep"));
	}
	
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] values = value.toString().split(",");
		String coords = key.toString();
		String month = values[0];
		String avgMonth = values[1];
		String avgMax = values[2];
		String avgMin = values[3];
		

		String cellCoord = Grid.getGridCoord(coords,xStep,yStep);
		
		output.collect(new Text(cellCoord), new Text(month+","+avgMonth+","+avgMax+","+avgMin));
	}
}