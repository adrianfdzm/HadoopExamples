package es.afm.hadoop.examples.writables.avgaggregation2d;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import es.afm.hadoop.examples.writables.AverageWritable;
import es.afm.hadoop.examples.writables.PointWritable;

public class AvgAggregation2DReducer extends Reducer<PointWritable, AverageWritable, PointWritable, AverageWritable> {

	@Override
	protected void reduce(PointWritable key, Iterable<AverageWritable> values,
			Reducer<PointWritable, AverageWritable, PointWritable, AverageWritable>.Context context)
					throws IOException, InterruptedException {
		long count = 0;
		long sum = 0;
		for (AverageWritable value : values) {
			count += value.getCount().get();
			sum += value.getSum().get();
		}
		context.write(key, new AverageWritable(count, sum));
	}
}
