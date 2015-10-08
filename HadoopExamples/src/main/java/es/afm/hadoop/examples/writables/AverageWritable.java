package es.afm.hadoop.examples.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class AverageWritable implements Writable {
	private LongWritable count;
	private LongWritable sum;

	public AverageWritable(long count, long sum) {
		this.count = new LongWritable(count);
		this.sum = new LongWritable(sum);
	}

	// init method without params must be present
	public AverageWritable() {
		count = new LongWritable();
		sum = new LongWritable();
	}

	public LongWritable getCount() {
		return count;
	}

	public void setCount(LongWritable count) {
		this.count = count;
	}
	
	public void setCount(long count) {
		this.count.set(count);
	}

	public LongWritable getSum() {
		return sum;
	}

	public void setSum(LongWritable sum) {
		this.sum = sum;
	}

	public void setSum(long sum) {
		this.sum.set(sum);
	}

	public double getAverage() {
		if (count.get() > 0)
			return sum.get() / (double) count.get();
		return 0.0;
	}

	public void readFields(DataInput in) throws IOException {
		count.readFields(in);
		sum.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		count.write(out);
		sum.write(out);
	}

	@Override
	public String toString() {
		return String.valueOf(this.getAverage());
	}

	/* MRUnit needs hashcode and equals in order to compare outputs */
	@Override
	public int hashCode() {
		return count.hashCode() * 128 + sum.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AverageWritable) {
			AverageWritable avg = (AverageWritable) obj;
			if (this.count.get() == avg.count.get()
					&& this.sum.get() == avg.sum.get())
				return true;
		}
		return false;
	}
}
