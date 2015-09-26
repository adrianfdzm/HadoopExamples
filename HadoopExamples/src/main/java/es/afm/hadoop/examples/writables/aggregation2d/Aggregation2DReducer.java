package es.afm.hadoop.examples.writables.aggregation2d;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.afm.hadoop.examples.writables.PointWritable;

public class Aggregation2DReducer extends Reducer<PointWritable, LongWritable, PointWritable, LongWritable> {

	@Override
	protected void reduce(PointWritable key, Iterable<LongWritable> values,
			Reducer<PointWritable, LongWritable, PointWritable, LongWritable>.Context context)
					throws IOException, InterruptedException {
		long count = 0;
		for (LongWritable value : values) {
			count += value.get();
		}
		context.write(key, new LongWritable(count));
	}
}
