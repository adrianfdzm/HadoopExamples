package es.afm.hadoop.examples.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class PointWritable implements WritableComparable<PointWritable> {
	private DoubleWritable first;
	private DoubleWritable second;
	
	public PointWritable(double first, double second) {
		this.first = new DoubleWritable(first);
		this.second = new DoubleWritable(second);
	}
	
	/*
	 * init method without params must be present
	 */
	public PointWritable() {
		first = new DoubleWritable();
		second = new DoubleWritable();
	}
	
	public DoubleWritable getFirst() {
		return first;
	}

	public void setFirst(DoubleWritable first) {
		this.first = first;
	}

	public DoubleWritable getSecond() {
		return second;
	}

	public void setSecond(DoubleWritable second) {
		this.second = second;
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public int compareTo(PointWritable arg0) {
		if(first.compareTo(arg0.getFirst()) == 0)
			return second.compareTo(arg0.getSecond());
		return first.compareTo(arg0.getFirst());
	}
	
	@Override
	public String toString() {
		return "[" + first.get() + ", " + second.get() + "]";
	}
	
	/*MRUnit needs hashcode and equals in order to compare outputs*/
	@Override
	public int hashCode() {
		return first.hashCode()*128 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PointWritable){
			if(this.compareTo((PointWritable) obj) == 0)
				return true;
		}
		return false;
	}

}
