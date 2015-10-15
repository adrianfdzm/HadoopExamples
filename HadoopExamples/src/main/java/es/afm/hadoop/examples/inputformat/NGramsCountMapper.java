package es.afm.hadoop.examples.inputformat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NGramsCountMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	private final LongWritable one = new LongWritable(1);
	private Text outKey = new Text();
	private int n = 1;
	private LinkedList<String> ngram = new LinkedList<String>();
	private StringBuilder sb = new StringBuilder();
	private static final String WHITESPACE = " ";

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		n = context.getConfiguration().getInt("ngram.size", n);
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String[] splits = value.toString().toLowerCase()
				.replaceAll("\\W", WHITESPACE).trim().split("\\s+");
		for (String split : splits) {
			ngram.add(split);
			if (ngram.size() == n) {
				outKey.set(buildNGram(ngram));
				ngram.removeFirst();
				context.write(outKey, one);
			}
		}
	}

	private String buildNGram(List<String> ngrams) {
		sb.setLength(0);
		for (String ngram : ngrams) {
			sb.append(ngram).append(WHITESPACE);
		}
		return sb.toString().trim();
	}
}
