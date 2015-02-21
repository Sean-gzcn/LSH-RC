package au.edu.uts.SaCFRAPP.preprocess;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SelectReducer extends Reducer<IntWritable, Text, Text, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		Iterator<Text> itrOfValues = values.iterator();
		while (itrOfValues.hasNext()) {
			context.write(itrOfValues.next(), null);
		}
	} 
}
