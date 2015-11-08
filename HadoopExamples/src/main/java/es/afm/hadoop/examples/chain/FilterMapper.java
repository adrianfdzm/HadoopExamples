package es.afm.hadoop.examples.chain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	private String sep = "\\s+";
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		//Filter out lines with less than 3 words
		String line = value.toString();
		String[] words = line.split(sep);
		if(words.length > 3) {
			context.write(key, value);
		}
	}
}
