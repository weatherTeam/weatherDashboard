package weather.wikipedia.infoExtraction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * Simple tuple class to store Weather Events detected in Wikipedia
 * {@link #title} is the title of the Wikipedia entry 
 * {@link #category} is the category (one from list {@link WikiFilter#keywords}) of the entry
 * {@link #startDate} and {@link #endDate} denote the start and the end of the event 
 * {@link #location} is the the extracted location of the event
 */
class WeatherEvent implements Writable {

	Text title;
	Text category;
	Text startDate;
	Text endDate;
	Text location;

	public WeatherEvent(String title, String category, String startDate,
			String endDate, String location) {
		this.title = new Text(title);
		this.category = new Text(category);
		this.startDate = new Text(startDate);
		this.endDate = new Text(endDate);
		this.location = new Text(location);
	}

	/**
	 * Used by Hadoop for reflexion
	 */
	public WeatherEvent() {
		this.title = new Text("");
		this.category = new Text("");
		this.startDate = new Text("");
		this.endDate = new Text("");
		this.location = new Text("");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		title.readFields(in);
		category.readFields(in);
		startDate.readFields(in);
		endDate.readFields(in);
		location.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		title.write(out);
		category.write(out);
		startDate.write(out);
		endDate.write(out);
		location.write(out);
	}

	@Override
	public String toString() {
		return title.toString() + "\t" + category.toString() + "\t"
				+ startDate.toString() + "\t" + endDate.toString() + "\t"
				+ location.toString();
	}

}
