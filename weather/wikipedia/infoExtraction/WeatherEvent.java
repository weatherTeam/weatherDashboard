package weather.wikipedia.infoExtraction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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

}
