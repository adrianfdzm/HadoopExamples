package es.afm.hadoop.examples.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideJoinReducer extends Reducer<CompositeKeyWritable, Text, NullWritable, Text> {
	private static final String SEP = ",";
	private Text outValue = new Text();
	
	@Override
	protected void reduce(CompositeKeyWritable key, Iterable<Text> values,
			Reducer<CompositeKeyWritable, Text, NullWritable, Text>.Context context)
					throws IOException, InterruptedException {
		boolean first = true;
		String dptoName = null;
		StringBuilder sb = new StringBuilder();
		String linea;
		for( Text value: values) {
			if(first) {
				//department: dpto-id, dpto-name
				first = false;
				dptoName = value.toString().split(SEP)[1];
			} else {
				//teachers: teacher-id,dpto-id,teacher-name,salary
				sb.append(dptoName).append(SEP);
				linea = value.toString();
				sb.append(linea.split(SEP)[2]).append(SEP); //teacher-name
				sb.append(linea.split(SEP)[3]);//salary
				
				outValue.set(sb.toString());
				context.write(NullWritable.get(), outValue);
				sb.setLength(0);//empty the sb
			}
		}
		
	}
}
