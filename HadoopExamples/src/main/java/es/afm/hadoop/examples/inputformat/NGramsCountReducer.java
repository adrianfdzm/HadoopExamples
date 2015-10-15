package es.afm.hadoop.examples.inputformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramsCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable outValue = new LongWritable();
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long count = 0;
		for (LongWritable value : values) {
			count += value.get();
		}
		outValue.set(count);
		context.write(key, outValue);
	}
}
