package au.edu.uts.SaCFRAPP.LSH;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QIQualityMeasureReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		double sum = 0.0;
		Iterator<DoubleWritable> itr = values.iterator();
		while(itr.hasNext()){
			sum += itr.next().get();
		}
		context.write(key, new DoubleWritable(sum));
	}
}
