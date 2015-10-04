package es.afm.hadoop.examples.reducesidejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyWritableGroupingComparator extends WritableComparator {
	
	protected CompositeKeyWritableGroupingComparator() {
		super(CompositeKeyWritable.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKeyWritable w1 = (CompositeKeyWritable) a;
		CompositeKeyWritable w2 = (CompositeKeyWritable) b;
		return w1.getJoinKey().compareTo(w2.getJoinKey());
	}

}
