package es.afm.hadoop.examples.partitioner.preprocess;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.afm.hadoop.examples.writables.counters.Counters;

public class PreProcessorMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	private Text word = new Text();
	private LongWritable nOccurrences = new LongWritable();
	private static final String SEP = "\\s+";

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(SEP);
		if (tokens == null || tokens.length != 2) {
			context.getCounter(Counters.MALFORMED_RECORD).increment(1);
		}
		long nbOccurrences = Long.parseLong(tokens[1]);
		word.set(tokens[0]);
		nOccurrences.set(nbOccurrences);
		context.write(nOccurrences, word);
	}
}
