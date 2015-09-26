package es.afm.hadoop.examples.writables.aggregation2d;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.afm.hadoop.examples.writables.PointWritable;
import es.afm.hadoop.examples.writables.counters.Counters;

public class Aggregation2DMapper extends Mapper<LongWritable, Text, PointWritable, LongWritable> {

	public static final String KEY_VALUE_SEP = "\\:";
	public static final String POINT_COORDINATE_SEP = ",";

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PointWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {

		try {
			String pointStr = value.toString().split(KEY_VALUE_SEP)[0];
			LongWritable outValue = new LongWritable(Long.parseLong(value.toString().split(KEY_VALUE_SEP)[1]));

			PointWritable outKey = new PointWritable(Double.parseDouble(pointStr.split(POINT_COORDINATE_SEP)[0]),
					Double.parseDouble(pointStr.split(POINT_COORDINATE_SEP)[1]));

			context.write(outKey, outValue);
		} catch (NumberFormatException e) {
			context.getCounter(Counters.WRONG_VALUE).increment(1);
		} catch (ArrayIndexOutOfBoundsException e) {
			context.getCounter(Counters.MALFORMED_RECORD).increment(1);
		}
	}
}
