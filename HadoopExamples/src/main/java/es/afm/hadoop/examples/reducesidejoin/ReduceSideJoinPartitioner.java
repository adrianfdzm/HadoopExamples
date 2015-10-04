package es.afm.hadoop.examples.reducesidejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ReduceSideJoinPartitioner extends Partitioner<CompositeKeyWritable, Text> {

	@Override
	public int getPartition(CompositeKeyWritable key, Text value, int numPartitions) {
		return Math.abs(key.getJoinKey().hashCode()) % numPartitions;
	}

}
