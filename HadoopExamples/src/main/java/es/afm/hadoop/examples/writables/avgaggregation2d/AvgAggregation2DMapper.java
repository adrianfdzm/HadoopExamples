package es.afm.hadoop.examples.writables.avgaggregation2d;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.afm.hadoop.examples.writables.AverageWritable;
import es.afm.hadoop.examples.writables.PointWritable;
import es.afm.hadoop.examples.writables.counters.Counters;

public class AvgAggregation2DMapper extends
		Mapper<LongWritable, Text, PointWritable, AverageWritable> {

	private static final String KEY_VALUE_SEP = "\\:";
	private static final String POINT_COORDINATE_SEP = ",";
	private AverageWritable outValue = new AverageWritable();
	private LongWritable one = new LongWritable(1);
	private PointWritable outKey = new PointWritable();

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, PointWritable, AverageWritable>.Context context)
			throws IOException, InterruptedException {

		try {
			String pointStr = value.toString().split(KEY_VALUE_SEP)[0];
			outValue.setCount(one);
			outValue.setSum(Long.parseLong(value.toString()
					.split(KEY_VALUE_SEP)[1]));

			outKey.setFirst(Double.parseDouble(pointStr
					.split(POINT_COORDINATE_SEP)[0]));
			outKey.setSecond(Double.parseDouble(pointStr
					.split(POINT_COORDINATE_SEP)[1]));

			context.write(outKey, outValue);
		} catch (NumberFormatException e) {
			context.getCounter(Counters.WRONG_VALUE).increment(1);
		} catch (ArrayIndexOutOfBoundsException e) {
			context.getCounter(Counters.MALFORMED_RECORD).increment(1);
		}
	}
}
