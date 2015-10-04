package es.afm.hadoop.examples.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.afm.hadoop.examples.writables.counters.Counters;

public class TeachersMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, Text> {

	private static final String SEP = ",";
	private CompositeKeyWritable outKey = new CompositeKeyWritable();

	@Override
	protected void setup(Mapper<LongWritable, Text, CompositeKeyWritable, Text>.Context context)
			throws IOException, InterruptedException {
		outKey.setDatasetKey(new IntWritable(CompositeKeyWritable.DATASET_KEY_TEACHERS));
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CompositeKeyWritable, Text>.Context context)
					throws IOException, InterruptedException {
		try {
			// teacher-id,dpto-id,teacher-name,salary
			outKey.setJoinKey(new Text(value.toString().split(SEP)[1]));
			context.write(outKey, value);
		} catch (Exception e) {
			context.getCounter(Counters.MALFORMED_RECORD).increment(1);
		}
	}
}
