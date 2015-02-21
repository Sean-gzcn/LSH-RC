package au.edu.uts.SaCFRAPP.preprocess;

import java.io.IOException;
import java.util.Vector;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.edu.uts.SaCFRAPP.common.Common;


public class CountMapper extends Mapper<LongWritable, Text, WritableRawKey, IntWritable> {
	
	IntWritable one = new IntWritable(1);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		Vector<String> rawValues = Common.rawStrToVector(value.toString());
		if(value.toString().contains("?")){
			context.getCounter(UserCounters.INCOMPLETE).increment(1);
		}
		
		//if (rawValues.size() != 43){
		//	context.getCounter(UserCounters.WRONG_SIZE).increment(1);
			//System.err.println("Size is: "+rawValues.size());
			//System.err.println("Malformat records:\n"+rawValues.toString());
		//}
		//else{
			for(int i=0; i<rawValues.size(); i++){
				WritableRawKey rkey = new WritableRawKey(String.valueOf(i), rawValues.get(i));
				context.write(rkey, one);
			}
		//}
	}
	
}