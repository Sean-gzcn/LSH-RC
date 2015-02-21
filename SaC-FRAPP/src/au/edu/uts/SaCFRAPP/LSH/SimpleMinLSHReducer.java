package au.edu.uts.SaCFRAPP.LSH;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SimpleMinLSHReducer extends Reducer<Text, Text, Text, Text>{

	private MultipleOutputs<Text, Text> multiOutputs;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		multiOutputs =  new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		context.getCounter(GroupCounters.ORIGINAL_GROUPS).increment(1);
		
		for(Text value: values){
			multiOutputs.write(key, value, key.toString()+"/part");
		}
		
		//Iterator<Text> itr = values.iterator();
		//while(itr.hasNext()){
		//	context.write(key, itr.next());
		//}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		multiOutputs.close();
	}
}
