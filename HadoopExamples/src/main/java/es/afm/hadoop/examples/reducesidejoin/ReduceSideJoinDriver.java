package es.afm.hadoop.examples.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

/**
 * ReduceSideJoin between 2 datasets teachers and departments
 */
public class ReduceSideJoinDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("ReduceSideJoinDriver required params: <teachers-path> <departments-path> <output_path>");
			System.exit(-1);
		}

		deleteOutputFileIfExists(args);

		final Job job = Job.getInstance(getConf());
		job.setJarByClass(ReduceSideJoinDriver.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setPartitionerClass(ReduceSideJoinPartitioner.class);
		job.setGroupingComparatorClass(CompositeKeyWritableGroupingComparator.class);

		job.setReducerClass(ReduceSideJoinReducer.class);

		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, TeachersMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, DepartmentsMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);

		return 0;
	}

	private void deleteOutputFileIfExists(String[] args) throws IOException {
		final Path output = new Path(args[2]);
		FileSystem.get(output.toUri(), getConf()).delete(output, true);
	}

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		ToolRunner.run(new ReduceSideJoinDriver(), args);
	}

}
