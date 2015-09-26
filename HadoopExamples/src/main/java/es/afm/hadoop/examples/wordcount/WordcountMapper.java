package es.afm.hadoop.examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private final LongWritable one = new LongWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String[] splits = value.toString().trim().split("\\s+");
		for (String split : splits) {
			context.write(new Text(split), one);
		}
	}
}
