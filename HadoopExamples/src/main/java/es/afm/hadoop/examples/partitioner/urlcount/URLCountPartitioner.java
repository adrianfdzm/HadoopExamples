package es.afm.hadoop.examples.partitioner.urlcount;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class URLCountPartitioner extends Partitioner<Text, LongWritable>{

	@Override
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		System.out.println(numPartitions);
		if(numPartitions == 0)
			return 0;
		try {
			URL url = new URL(key.toString());
			return url.getHost().hashCode()%numPartitions;
		} catch (MalformedURLException ignored) {
			return 0;
		}
	}

}
