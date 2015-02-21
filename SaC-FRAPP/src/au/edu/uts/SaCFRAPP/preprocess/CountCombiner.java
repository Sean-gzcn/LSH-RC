package au.edu.uts.SaCFRAPP.preprocess;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class CountCombiner extends Reducer<WritableRawKey, IntWritable, WritableRawKey, IntWritable> {

	@Override
	public void reduce(WritableRawKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		Iterator<IntWritable> itrOfValues = values.iterator();
		while (itrOfValues.hasNext()) {
			IntWritable value = (IntWritable) itrOfValues.next();
			sum += value.get();
		}
		
		context.write(key, new IntWritable(sum));
	}
	
}