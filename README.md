# HadoopExamples
This code was built in order to be used as example of all the characteristic of Hadoop MapReduce framework. Next, there is a brief description of each example.
* **Wordcount**: The most basic example of MapReduce job
* **writables/Aggregation2D**: This example aggregates 2-dimension measurements in order to show how to use Hadoop writables
* **writables/AvgAggregation2D**: Another example on 2-dimension measurements that show the use of Hadoop writables
* **partitioner/UrlCount**: This example shows how partitioning functionality can be overriden in order group keys in the desired reduce task. The urls from a file are counted but we desire that urls with the same host end up in the same output file
* **partitioner/TotalOrderV1**: It shows how to order a wordcount file by the number of occurrences. Hadoop orders the output of each reduce task but we want the output of all reduce task to be ordered (part-r-00000 registers < part-r-00001 registers < part-r-00002 registers < ...). This job shows an ugly solution for a 2 reduce tasks job
* **partitioner/ToatlOrderV2**: Try to solve the same problem exposed above but this time Hadoop ```Sampler``` and ```TotalOrderPartitioner``` classes are used

##Wordcount
Basic example of MapReduce job. Map split lines into words that are writen as key. Punctuation marks are not taken into account
```java
  	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String[] splits = value.toString().trim().split("\\s+");
		for (String split : splits) {
			context.write(new Text(split), one);
		}
	}
```
Reduce aggregates the number of occurrences of each word
```java
	 @Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		long count = 0;
		for (LongWritable value : values) {
			count += value.get();
		}
		context.write(key, new LongWritable(count));
	}
```
WordcountReducer class is used as a combiner to aggreagate map output locally
```java
job.setCombinerClass(WordcountReducer.class);
```
In order to run the MapReduce job using ```hadoop jar``` command, main class musts implement ```Tool``` interface and extend ```Configured```. This is shown on ```WordcountDriver``` class

##writables/Aggregation2D

This MapReduce job aims to illustrate how to use Hadoop custom ```WritableComparable``` object inside a MapReduce job. Map coordinates that are readed from a plain text file are seriealized into  ```PointWritable``` objects during the map tasks and passed on to the reduce tasks
```java
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
```

Note that error handling are performed incrementing some Hadoop counters. Counters are automatically aggregated over Map and Reduce phases. They are defined using java ```Enumeration```. The most natural use of that is to use counters to track the number of malformed records. But counters can actually be used for any kind of other statistics on your records. ```PointWritable``` class tells Hadoop how the 2-dimension information that is being handled has to be serialized during the MapReduce job
```java
public class PointWritable implements WritableComparable<PointWritable> {
	private DoubleWritable first;
	private DoubleWritable second;
	
	public PointWritable(double first, double second) {
		this.first = new DoubleWritable(first);
		this.second = new DoubleWritable(second);
	}
	
	/*
	 * init method without params must be present
	 */
	public PointWritable() {
		first = new DoubleWritable();
		second = new DoubleWritable();
	}
	
	public DoubleWritable getFirst() {
		return first;
	}

	public void setFirst(DoubleWritable first) {
		this.first = first;
	}

	public DoubleWritable getSecond() {
		return second;
	}

	public void setSecond(DoubleWritable second) {
		this.second = second;
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public int compareTo(PointWritable arg0) {
		if(first.compareTo(arg0.getFirst()) == 0)
			return second.compareTo(arg0.getSecond());
		return first.compareTo(arg0.getFirst());
	}
	
	@Override
	public String toString() {
		return "[" + first.get() + ", " + second.get() + "]";
	}
	
	/*MRUnit needs hashcode and equals in order to compare outputs*/
	@Override
	public int hashCode() {
		return first.hashCode()*128 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof PointWritable){
			if(this.compareTo((PointWritable) obj) == 0)
				return true;
		}
		return false;
	}

}
```
Note that:
* A default constructor without arguments must be present because it is used by the MapReduce framework. 
* There is a ```Writable``` interface but, in order to use our custom ```Writable``` as key, it is needed to implement the ```WritableComparable``` interface
* ```equals()``` and ```hashCode()``` methods are used by some tools such as MRUnit (Unit testing for Hadoop MapReduce)
* ```toString()``` method is used to write to the final output file of the job

##writables/AvgAggregation2D

##partitioner/UrlCount

##partitioner/TotalOrderSortV1

##partitioner/TotalOrderSortV2
