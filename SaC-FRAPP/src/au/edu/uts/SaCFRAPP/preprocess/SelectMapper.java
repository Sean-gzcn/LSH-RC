package au.edu.uts.SaCFRAPP.preprocess;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import au.edu.uts.SaCFRAPP.common.Common;

public class SelectMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	IntWritable one = new IntWritable(1);
	int[] selectedQIs = {0, 24, 31, 41, 12, 4, 7, 19, 22};
	int[] selectedSAs = {25, 40, 8};
	String[] strs = {"01", "02", "03", "04", "05", "06", "07", "08", "09"};
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		Vector<String> rawValues = Common.rawStrToVector(value.toString());
		
		if(value.toString().contains("?")){
			context.getCounter(UserCounters.INCOMPLETE).increment(1);
		}
		
		if (rawValues.size() != 43){
			context.getCounter(UserCounters.WRONG_SIZE).increment(1);
		}
		else{
			if(Integer.valueOf(rawValues.get(40)) != 0){
				Vector<String> selectedValues = new Vector<String>();
				//for(int j = 0; j<selectedQIs.length; j++){
				//	selectedValues.add(rawValues.get(selectedQIs[j]));
				//}
				//for(int j = 0; j<selectedSAs.length; j++){
				//	selectedValues.add(rawValues.get(selectedSAs[j]));
				//}
				
				for(int j = 0; j<5; j++){
					selectedValues.add(rawValues.get(selectedQIs[j]));
				}
				for(int j = 5; j<selectedQIs.length; j++){
					selectedValues.add(Common.vtMap.get(rawValues.get(selectedQIs[j])));
				}
				
				String tmp = rawValues.get(selectedSAs[0]).trim();
				for (int k = 0; k<strs.length; k++){
					if(tmp.equals(strs[k])){
						tmp = String.valueOf((k+1)*10);
						break;
					}
				}
				selectedValues.add(tmp);
				
				selectedValues.add(rawValues.get(selectedSAs[1]));
				selectedValues.add(Common.vtMap.get(rawValues.get(selectedSAs[2])));
				
				
				context.write(one, new Text(Common.vectorToString(selectedValues)));
			}
		}
	}
	

}
