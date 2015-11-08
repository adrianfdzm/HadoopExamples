package es.afm.hadoop.examples.distributedcache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.afm.hadoop.examples.writables.counters.Counters;

public class MapSideJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private Map<Integer, String> dptosMap = new HashMap<Integer, String>();
	private Text outValue = new Text();
	private String SEP = ",";

	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		URI dptosFile = context.getCacheFiles()[0];
		fillDptosMap(dptosFile, context);
	}

	private void fillDptosMap(URI dptosFile, Mapper<LongWritable, Text, NullWritable, Text>.Context context) {
		BufferedReader br = null;
		try {
			System.out.println(dptosFile);
			br = new BufferedReader(
					new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(dptosFile))));
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
				String[] splits = line.split(SEP);
				try {
					dptosMap.put(Integer.parseInt(splits[0]), splits[1]);
				} catch (ArrayIndexOutOfBoundsException e) {
					context.getCounter(Counters.MALFORMED_RECORD).increment(1);
				} catch (NumberFormatException e) {
					context.getCounter(Counters.WRONG_VALUE).increment(1);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (Exception ignored) {
				}
			}
		}

	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(value.toString().split(SEP)[2]).append(SEP); // teacher-name
			sb.append(dptosMap.get(Integer.parseInt(value.toString().split(SEP)[1]))).append(SEP); // dpto-name
			sb.append(value.toString().split(SEP)[3]);// salary

			outValue.set(sb.toString());

			context.write(NullWritable.get(), outValue);
		} catch (NumberFormatException e) {
			context.getCounter(Counters.WRONG_VALUE).increment(1);
		}
	}

}
