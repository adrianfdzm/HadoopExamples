package es.afm.hadoop.examples.partitioner.urlcount;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.afm.hadoop.examples.partitioner.urlcount.counters.Counters;

public class URLCountMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	private final LongWritable one = new LongWritable(1);

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {

		try {
			@SuppressWarnings("unused")
			URL url = new URL(value.toString());

			context.write(value, one);
		} catch (MalformedURLException e) {
			context.getCounter(Counters.MALFORMED_URL).increment(1);
		}

	}
}
