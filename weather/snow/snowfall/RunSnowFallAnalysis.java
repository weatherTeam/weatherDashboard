package weather.snow.snowfall;

/**
 * Created by Jonathan Duss on 26.04.14.
 *
 * This class run the DailySnowFallEstimation, and uses its output to convert it to weekly snowfall. The final output
 * it the weekly snowfall, or so-called weekly cumulation
 *
 * It run the analysis on the dataset of the given YEAR. If an end year is given, it compute for every data from YEAR to
 * END YEAR
 */
public class RunSnowFallAnalysis {

	public static void main(String[] args){

		if(args.length != 3){
			//System.err.print("Need 3 mandatory arguments + 1 optional argument: inputDirectory, tempDirectory, outputDirectory, [year], [end year] ");
			System.err.print("Need 3 mandatory arguments: inputDirectory, tempDirectory, outputDirectory");
			System.exit(1);
		}


		//First launch dailySnowFallEstimation, then use the output and compute the weekly snowfalls
		try {
			String firstMapRedPath[] = new String[] {args[0] , args[1]};
			DailySnowFallEstimation.main(firstMapRedPath);

			String secondMapRedPath[] = new String[] {args[1] , args[2]};
			DailyToWeeklySnowFallEstimation.main(secondMapRedPath);
		} catch (Exception e) {
			e.printStackTrace();
		}


	}
}
