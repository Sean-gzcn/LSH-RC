package au.edu.uts.SaCFRAPP.LSH;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import au.edu.uts.SaCFRAPP.common.Common;

public class MinLSHReducer extends Reducer<Text, Text, Text, Text>{

	HashMap<Integer, Vector<RecordGroupPair>> hm;
	int lenOfHashtable;
	
	HashMap<String, Vector<String>> groups;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		hm = new HashMap<Integer, Vector<RecordGroupPair>>();
		lenOfHashtable = context.getConfiguration().getInt("HashTableSize", -1);
		lenOfHashtable = MinHash.getPrimeNotLessThan(lenOfHashtable);
		groups = new HashMap<String, Vector<String>>();
	}
	
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		context.getCounter(GroupCounters.ORIGINAL_GROUPS).increment(1);
		
		// Find the group they should go to according to records by look up in record-group index
		String groupID = key.toString();
		Vector<String> records = new Vector<String>();
		Vector<Integer> hashValues = new Vector<Integer>();
		Iterator<Text> itr = values.iterator();
		while(itr.hasNext()){
			String record = itr.next().toString();
			records.add(record);
			hashValues.add(this.hash(Common.getQIString(record)));
		}
		
		Boolean groupHit = false;
		for(int i = 0; i<hashValues.size(); i++){
			int hashValue = hashValues.get(i);
			if(hm.containsKey(hashValue)){
				String record = records.get(i);
				Vector<RecordGroupPair> pairList = hm.get(hashValue);
				Iterator<RecordGroupPair> pairItr  = pairList.iterator();
				while(pairItr.hasNext()){
					RecordGroupPair pair = pairItr.next();
					if(Common.getQIString(pair.getRecord()).equals(Common.getQIString(record))){
						groupID = pair.getGroup();
						groupHit = true;
						break;
					}
				}
				if(groupHit){
					break;
				}
			}
		}
		
		// Go to the group as a whole
		if(groupHit){
			Vector<String> groupRecords = groups.get(groupID);
			for(int i = 0; i<records.size(); i++){
				String record = records.get(i);
				Boolean repeated = false;
				for(int j = 0; j<groupRecords.size(); j++){
					if(Common.getQIString(groupRecords.get(j)).equals(Common.getQIString(record))){
						repeated = true;
						break;
					}
				}
				if(!repeated){
					groupRecords.add(record);
				}
			}
		}
		else{
			groups.put(groupID, records);
		}
		
		
		// Update record-group index
		for(int i = 0; i<records.size(); i++){
			groupHit = false;
			String record = records.get(i);
			int hashValue = hashValues.get(i);
			if(hm.containsKey(hashValue)){
				Vector<RecordGroupPair> pairList = hm.get(hashValue);
				Iterator<RecordGroupPair> pairItr  = pairList.iterator();
				while(pairItr.hasNext()){
					RecordGroupPair pair = pairItr.next();
					if(Common.getQIString(pair.getRecord()).equals(Common.getQIString(record))){
						groupHit = true;
						break;
					}
				}
				if(!groupHit){
					pairList.add(new RecordGroupPair(record, groupID));
				}
			}
			else{
				Vector<RecordGroupPair> pairList = new Vector<RecordGroupPair>();
				pairList.add(new RecordGroupPair(record, groupID));
				hm.put(hashValue, pairList);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		//Iterator<Vector<RecordGroupPair>> itr = hm.values().iterator();
		//while(itr.hasNext()){
		//	Vector<RecordGroupPair> pairs = itr.next();
		//	Iterator<RecordGroupPair> pairItr = pairs.iterator();
		//	while(pairItr.hasNext()){
		//		RecordGroupPair pair = pairItr.next();
		//		context.write(new Text(pair.getGroup()), new Text(pair.getRecord()));
		//	}
		//}
		Iterator<String> itr = groups.keySet().iterator();
		while(itr.hasNext()){
			
			context.getCounter(GroupCounters.REFINED_GROUPS).increment(1);
			
			String key = itr.next();
			Iterator<String> itrRecord = groups.get(key).iterator();
			while(itrRecord.hasNext()){
				context.write(new Text(key), new Text(itrRecord.next()));
			}
		}
	}
	
	public int hash(String str){
		return Math.abs(str.hashCode())%lenOfHashtable;
	}
	
	public class RecordGroupPair{
		String record;
		String group;
		
		public RecordGroupPair(){
			
		}
		
		public RecordGroupPair(String record, String group){
			this.record = record;
			this.group = group;
		}
		
		public void setRecord(String record){
			this.record = record;
		}
		
		public void setGroup(String group){
			this.group = group;
		}
		
		public String getRecord(){
			return record;
		}
		
		public String getGroup(){
			return group;
		}
	}
}
