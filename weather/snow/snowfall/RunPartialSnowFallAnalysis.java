package weather.snow.snowfall;

import weather.snow.snowStatistics.MonthlySnowCumulationTrend;

/**
 * Created by Jonathan Duss on 26.04.14.
 *
 * This assume the RunCompleteSnowFallAnalysis (the part DailySnowFall Estimation) was launched.
 * It uses its output.
 *
 *
 */
public class RunPartialSnowFallAnalysis {

	public static void main(String[] args){

		if(args.length != 2){
			System.err.print("Need 2 mandatory arguments: inputDirectory, outputDirectory");
			System.exit(1);
		}


		//First launch dailySnowFallEstimation, then use the output and compute the weekly snowfalls
		try {

			String secondMapRedPath[] = new String[] {args[0] , args[1] + "/weekly"};
			DailyToWeeklySnowFallEstimation.main(secondMapRedPath);

			String thirdMapRedPath[] = new String[] {args[0] , args[1] + "/monthly"};
			DailyToMonthlySnowFallEstimation.main(thirdMapRedPath);

			String fourthMapRedPath[] = new String[] {args[1] + "/monthly" , args[1] + "/average"};
			MonthlySnowCumulationTrend.main(fourthMapRedPath);


		} catch (Exception e) {
			e.printStackTrace();
		}


	}
}
