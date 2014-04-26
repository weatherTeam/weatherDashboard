package weather.snow.snowfall;

/**
 * Created by Jonathan Duss on 26.04.14.
 */
public class SnowFall {

	public static void main(String[] args){

		if(args.length != 3){
			System.err.print("Need 3 arguments: inputDirectory, tempDirectory, outputDirectory");
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
