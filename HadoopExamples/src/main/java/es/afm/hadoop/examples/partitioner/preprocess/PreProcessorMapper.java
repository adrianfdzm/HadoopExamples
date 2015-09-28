package es.afm.hadoop.examples.partitioner.preprocess;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreProcessorMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private Text word = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split("\\s+");
		if( tokens == null || tokens.length != 2 ){
			System.err.print("Problem with input line: "+line+"n");
			return;
		}
		long nbOccurences = Long.parseLong(tokens[1]);
		word.set(tokens[0]);
		context.write(new LongWritable(nbOccurences),word );
	}
}
