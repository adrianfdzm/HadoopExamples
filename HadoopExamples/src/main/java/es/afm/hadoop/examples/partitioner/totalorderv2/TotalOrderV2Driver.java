package es.afm.hadoop.examples.partitioner.totalorderv2;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Read SequenceFiles (#occurrences,word) and perform a total order on them
 */
public class TotalOrderV2Driver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("TotalOrderV2 required params: <input_path> <output_path>");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = new Job(getConf());
		// local mode execution will turn this to 1
		job.setNumReduceTasks(2);

		job.setJarByClass(TotalOrderV2Driver.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// We may use a combiner
		job.setPartitionerClass(TotalOrderPartitioner.class);
		RandomSampler<LongWritable, Text> sampler = new InputSampler.RandomSampler<LongWritable, Text>(0.1, 100);
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[0] + "/_partition.lst"));
		InputSampler.writePartitionFile(job, sampler);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[1]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TotalOrderV2Driver(), args);
	}

}
