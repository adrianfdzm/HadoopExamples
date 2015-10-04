package es.afm.hadoop.examples.reducesidejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
	private Text joinKey;
	private IntWritable datasetKey;
	public static final int DATASET_KEY_TEACHERS = 2;
	public static final int DATASET_KEY_DEPARTMENTS = 1;

	/* Empty constructor needed */
	public CompositeKeyWritable() {
		joinKey = new Text();
		datasetKey = new IntWritable();
	}

	public CompositeKeyWritable(Text joinKey, IntWritable datasetKey) {
		this.joinKey = joinKey;
		this.datasetKey = datasetKey;
	}

	public Text getJoinKey() {
		return joinKey;
	}

	public void setJoinKey(Text joinKey) {
		this.joinKey = joinKey;
	}

	public IntWritable getDatasetKey() {
		return datasetKey;
	}

	public void setDatasetKey(IntWritable datasetKey) {
		this.datasetKey = datasetKey;
	}

	public void readFields(DataInput in) throws IOException {
		joinKey.readFields(in);
		datasetKey.readFields(in);

	}

	public void write(DataOutput out) throws IOException {
		joinKey.write(out);
		datasetKey.write(out);
	}

	public int compareTo(CompositeKeyWritable obj) {
		int result = joinKey.compareTo(obj.joinKey);
		if (result == 0)
			result = datasetKey.compareTo(obj.datasetKey);
		return result;
	}

	@Override
	public String toString() {
		return new StringBuilder().append(datasetKey).append(", ")
				.append(joinKey).toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof CompositeKeyWritable)
			return this.compareTo((CompositeKeyWritable) obj) == 0;
		return false;
	}

}
