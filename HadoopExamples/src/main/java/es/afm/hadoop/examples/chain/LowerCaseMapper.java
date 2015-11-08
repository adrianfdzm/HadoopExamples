package es.afm.hadoop.examples.chain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LowerCaseMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.toLowerCase();
		value.set(line);
		context.write(key, value);
	}
}
