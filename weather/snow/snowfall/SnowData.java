package weather.snow.snowfall;

import java.text.DecimalFormat;
import java.util.StringTokenizer;

/*
	Class storing snow data, and allowing to quickly pass these data through the mapper into a "standard" format and
	recover quickly in the reducer.

	Snow cumulation and depth are integers
	Precipitation is 2 decimals float

	Tempe
 */
class SnowData {

	public final static float NO_TEMPERATURE_PROVIDED = 30000f; //just need a fake number to notify that there is no
	// temperature that is provided (due to an error)
	public final static int NO_SNOW_INFO = -1;
	//public final int NO_SNOW_FALL_PROVIDED = -1;

	private String snowDepth;
	private String snowFallFromSnowDepth;
	private String snowFallFromRain;
	private String precipitation;
	private String temperature;

	DecimalFormat snowCumulationNumberFormatter = new DecimalFormat("###"); //


	//CONSTRUCTOR



	public SnowData() {
		snowDepth = "**";
		snowFallFromRain = "**";
		snowFallFromSnowDepth = "**";
		temperature = "**";
		precipitation = "**";
	}

	public SnowData(String data){
		// return temperature + "\t" + snowDepth + "\t" + snowFallFromSnowDepth + "\t" + precipitation + " \t" +
		// snowFallFromRain;
		this(); //set  default value in case the data are not in the correct format
		StringTokenizer st = new StringTokenizer(data,"\t");
		if (st.countTokens() == 5) {
			temperature = st.nextToken();
			snowDepth = st.nextToken();
			snowFallFromSnowDepth = st.nextToken();
			precipitation = st.nextToken();
			snowFallFromRain = st.nextToken();
		}
	}


	//GETTER AND SETTER
	public void setSnowDepth(int sn) {
		if (sn < 0) {
			snowDepth = "**";
		} else {
			snowDepth = "" + sn;
		}
	}

	public void setSnowFallFromSnowDepth(int sn) {
		if (sn < 0) {
			snowFallFromSnowDepth = "**";
		} else {
			snowFallFromSnowDepth = "" + sn;
		}
	}

	public void setSnowFallFromRain(float sn) {
		if (sn < 0) {
			snowFallFromRain = "**";
		} else {
			snowFallFromRain = "" + sn;
		}
	}

	public String getTemperature() {
		return temperature;
	}

	public void setTemperature(String temperature) {
		this.temperature = temperature;
	}

	public void setTemperature(float temp) {
		if (temp == NO_TEMPERATURE_PROVIDED) {
			temperature = "**";
		} else {
			temperature = "" + temp;
		}
	}

	public String getSnowDepth() {
		return snowDepth;
	}

	public void setSnowDepth(String snowDepth) {
		this.snowDepth = snowDepth;
	}

//	public String getSnowFallFromSnowDepth() {
//		return snowFallFromSnowDepth;
//	}

	public float getSnowFallFromSnowDepth(){
		if(snowFallFromSnowDepth.equals("**")){
			return NO_SNOW_INFO;
		}
		else return Float.parseFloat(snowFallFromSnowDepth);
	}

	public void setSnowFallFromSnowDepth(String snowFallFromSnowDepth) {
		this.snowFallFromSnowDepth = snowFallFromSnowDepth;
	}

//	public String getSnowFallFromRain() {
//		return snowFallFromRain;
//	}

	public float getSnowFallFromRain(){
		if(snowFallFromRain.equals("**")){
			return NO_SNOW_INFO;
		}
		else return Float.parseFloat(snowFallFromRain);
	}

	public void setSnowFallFromRain(String snowFallFromRain) {
		this.snowFallFromRain = snowFallFromRain;
	}

	public String getPrecipitation() {
		return precipitation;
	}

	public void setPrecipitation(String precipitation) {
		this.precipitation = precipitation;
	}

	public void setPrecipitation(float p) {
		if (p < 0) {
			this.precipitation = "**";
		} else {
			this.precipitation = "" + p;
		}
	}


	public String toString() {
		return temperature + "\t" + snowDepth + "\t" + snowFallFromSnowDepth + "\t" + precipitation + " \t" +
				snowFallFromRain;
	}

	public String toCumulationStrings(){
		return snowFallFromSnowDepth + "\t" + snowFallFromRain;
	}


}