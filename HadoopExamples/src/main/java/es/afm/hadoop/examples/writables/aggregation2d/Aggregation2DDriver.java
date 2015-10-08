package es.afm.hadoop.examples.writables.aggregation2d;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import es.afm.hadoop.examples.writables.PointWritable;
import es.afm.hadoop.examples.writables.counters.Counters;

/**
 * Read a 2D coordinate measures map and aggregate their values
 * coordinate1,coordinate2:value
 * 
 */
public class Aggregation2DDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("AvgAggregation2DDriver required params: <input_path> <output_path>");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = Job.getInstance(getConf());
		job.setJarByClass(Aggregation2DDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(Aggregation2DMapper.class);
		job.setReducerClass(Aggregation2DReducer.class);

		job.setMapOutputKeyClass(PointWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(PointWritable.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		System.out.println("# Malformed records: "
				+ job.getCounters().findCounter(Counters.MALFORMED_RECORD)
						.getValue());
		System.out.println("# Wrong values in records records: "
				+ job.getCounters().findCounter(Counters.WRONG_VALUE)
						.getValue());
		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		ToolRunner.run(new Aggregation2DDriver(), args);
	}

}
