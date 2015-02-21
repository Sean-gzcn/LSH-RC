package au.edu.uts.SaCFRAPP.preprocess;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class CountPartitioner extends Partitioner<WritableRawKey, IntWritable> {
	
	public CountPartitioner(){
	}

	@Override
	public int getPartition(WritableRawKey key, IntWritable value, int numOfPartitions) {
		
		//return key.getParent().hashCode()%numOfPartitions;
		return Math.abs(key.getAttribute().hashCode())%numOfPartitions;
	}

}
