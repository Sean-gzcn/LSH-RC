package au.edu.uts.SaCFRAPP.LSH;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.io.Text;

import au.edu.uts.SaCFRAPP.LSH.MinLSHReducer.RecordGroupPair;
import au.edu.uts.SaCFRAPP.PAClustering.WritableRecord;
import au.edu.uts.SaCFRAPP.common.Common;

public class SerialMinLSH {

	MinHash minHash;
	int numOfHashFunctions;
	int bands;
	int widthOfBand;
	int[][] hashFamily;
	HashMap<String, Integer> IDBucketIndex;
	Vector<Vector<String>> rawBuckets;
	
	HashMap<Integer, Vector<RecordGroupPair>> hm;
	HashMap<String, Vector<Integer>> groupHashIndex;
	int lenOfHashtable;
	
	HashMap<String, Vector<String>> groups;
	
	
	public SerialMinLSH(int numOfBands, int widthOfBand, int hashtableSize){
		
		minHash = new MinHash();
		
		bands = numOfBands;
		this.widthOfBand = widthOfBand;
		numOfHashFunctions = bands*this.widthOfBand;
		hashFamily = new int[numOfHashFunctions][3];
		
		Random r = new Random();
		for(int i = 0; i<numOfHashFunctions; i++){
			hashFamily[i][0] = r.nextInt();
			hashFamily[i][1] = r.nextInt();
			hashFamily[i][2] = r.nextInt();
		}
		
		IDBucketIndex = new HashMap<String, Integer>();  // Indexing for bucket by groupID
		rawBuckets = new Vector<Vector<String>>();
		
		
		hm = new HashMap<Integer, Vector<RecordGroupPair>>();  // Indexing for groupID by record
		groupHashIndex = new HashMap<String, Vector<Integer>>();  // Indexing the hash values of record by groupID
		lenOfHashtable = MinHash.getPrimeNotLessThan(hashtableSize);
		groups = new HashMap<String, Vector<String>>();
	}
	
	public Vector<Vector<String>> MinLSHHashing(Vector<String> records){
		this.hashing(records);
		this.merging();
		
		Vector<Vector<String>> resultVecs = new Vector<Vector<String>>();
		Iterator<Vector<String>> vecItr = groups.values().iterator();
		while(vecItr.hasNext()){
			resultVecs.add(vecItr.next());
		}
		return resultVecs;
	}
	
	private void hashing(Vector<String> records){
		
		Iterator<String> recordItr = records.iterator();
		String record;
		while(recordItr.hasNext()){
			record = recordItr.next();
			
			for(int i = 0; i<bands; i++){
				String bucketID = "";
				int j = 0;
				for(; j<widthOfBand-1; j++){
					int mHash = minHash.minHash(record, hashFamily[i*widthOfBand+j][0], 
							hashFamily[i*widthOfBand+j][1], hashFamily[i*widthOfBand+j][2]);
					bucketID = bucketID+mHash+"-";
				}
				int mHash = minHash.minHash(record, hashFamily[i*widthOfBand+j][0], 
						hashFamily[i*widthOfBand+j][1], hashFamily[i*widthOfBand+j][2]);
				bucketID = bucketID+mHash;
				
				int bucketIndex = this.getBucketIndex(bucketID);
				rawBuckets.get(bucketIndex).add(record);
			}
		}
	}
	
	private int getBucketIndex(String bucketID){
		
		Integer index = IDBucketIndex.get(bucketID);
		if(index == null){
			index = rawBuckets.size();
			rawBuckets.add(new Vector<String>());
			IDBucketIndex.put(bucketID, index);
		}
		
		return index;
	}
	
	private void merging(){
		
		Iterator<Entry<String, Integer>> entItr = IDBucketIndex.entrySet().iterator();
		while(entItr.hasNext()){
			
			Entry<String, Integer> entry = entItr.next();
			String groupID = entry.getKey();
			Vector<String> records = rawBuckets.get(entry.getValue());
			
			// Find the groups they should go to according to records by look up in record-group index
			Vector<Integer> hashValues = new Vector<Integer>();
			Iterator<String> recItr = records.iterator();
			while(recItr.hasNext()){
				String record = recItr.next();
				hashValues.add(this.hash(Common.getQIString(record)));
			}
			
			//Boolean groupHit = false;
			HashSet<String> groupIDs = new HashSet<String>();
			for(int i = 0; i<hashValues.size(); i++){
				int hashValue = hashValues.get(i);
				if(hm.containsKey(hashValue)){
					String record = records.get(i);
					Vector<RecordGroupPair> pairList = hm.get(hashValue);
					Iterator<RecordGroupPair> pairItr  = pairList.iterator();
					while(pairItr.hasNext()){
						RecordGroupPair pair = pairItr.next();
						if(Common.getQIString(pair.getRecord()).equals(Common.getQIString(record))){
							groupIDs.add(pair.getGroup());
							
							break;
						}
					}
				}
			}
			
			// Merge groups as a whole
			if(!groupIDs.isEmpty()){
			
				Iterator<String> setItr = groupIDs.iterator();
				groupID = setItr.next();
				Vector<String> groupRecords = groups.get(groupID);
			//if(groupRecords == null){
			//	System.out.println("groupRecords is null");
			//}
			//System.out.println("groupIDs is: "+groupIDs);
			//System.out.println("groupID is: "+groupID);
			//System.out.println("ADD: "+ groups.keySet());
			
				while(setItr.hasNext()){
					String tmpGroupID = setItr.next();
				//System.out.println("tmpID is: "+tmpGroupID);
					groupRecords.addAll(groups.get(tmpGroupID));
					groups.remove(tmpGroupID);
				//System.out.println("REM: "+ groups.keySet());
				}
				
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
			// For records in other groups
			if(groupIDs.size() > 1){
				Iterator<String> setItr = groupIDs.iterator();
				while(setItr.hasNext()){
					String tmpGroupID = setItr.next();
					if(!tmpGroupID.equals(groupID)){
						Iterator<Integer> intItr = hm.keySet().iterator();
						while(intItr.hasNext()){
							int hValue = intItr.next();
							Vector<RecordGroupPair> pairList = hm.get(hValue);
							Iterator<RecordGroupPair> pairItr  = pairList.iterator();
							while(pairItr.hasNext()){
								RecordGroupPair pair = pairItr.next();
								if(pair.getGroup().equals(tmpGroupID)){
									pair.setGroup(groupID);
								}
							}
						}
					}
				}
			}
			
			/*if(groupIDs.size() > 1){
				Iterator<String> setItr = groupIDs.iterator();
				while(setItr.hasNext()){
					String tmpGroupID = setItr.next();
					if(!tmpGroupID.equals(groupID)){
						Vector<Integer> hValues = groupHashIndex.get(tmpGroupID);
						Iterator<Integer> intItr  = hValues.iterator();
						while(intItr.hasNext()){
							int hValue = intItr.next();
							Vector<RecordGroupPair> pairList = hm.get(hValue);
							Iterator<RecordGroupPair> pairItr  = pairList.iterator();
							while(pairItr.hasNext()){
								RecordGroupPair pair = pairItr.next();
								if(pair.getGroup().equals(tmpGroupID)){
									pair.setGroup(groupID);
								}
							}
						}
					}
				}
			}*/
			
			// For new added records
			for(int i = 0; i<records.size(); i++){
				boolean groupHit = false;
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
						
						//this.getHashVector(groupID).add(hashValue);
					}
				}
				else{
					Vector<RecordGroupPair> pairList = new Vector<RecordGroupPair>();
					pairList.add(new RecordGroupPair(record, groupID));
					hm.put(hashValue, pairList);
					
					//this.getHashVector(groupID).add(hashValue);
				}
			}
			
			// Update group hash value index
			/*Iterator<String> setItr = groupIDs.iterator();
			while(setItr.hasNext()){
				String tmpGroupID = setItr.next();
				if(!tmpGroupID.equals(groupID)){
					groupHashIndex.remove(tmpGroupID);
				}
			}*/
		}
	}
	
	private Vector<Integer> getHashVector(String groupID){
		
		Vector<Integer> rltVector = groupHashIndex.get(groupID);
		if(rltVector == null){
			rltVector = new Vector<Integer>();
			groupHashIndex.put(groupID, rltVector);
		}
		
		return rltVector;
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
	
	public static void main(String[] args){
		Vector<String> records = new Vector<String>();
		//records.add("59, 12th, Married-AF-spouse, Priv-house-serv, Unmarried, Black, Male, Canada, StateGov");
		//records.add("42, 1st-4th, Married-AF-spouse, Protective-serv, Not-in-family, White, Male, Ecuador, StateGov");
		//records.add("29, Masters, Married-civ-spouse, Adm-clerical, Not-in-family, White, Male, Taiwan, Private");
		//records.add("30, Masters, Married-civ-spouse, Adm-clerical, Not-in-family, White, Male, Taiwan, Private");
		
		try{
			String dataPathStr = "dat/LSH/uniformed_10M.data";
			File file = new File(dataPathStr);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			
			String line;
			while((line = br.readLine()) != null){
				records.add(line.trim());
			}
			
			br.close();
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
		
		SerialMinLSH sms = new SerialMinLSH(2, 3, 10000);
		Vector<Vector<String>> rlt = sms.MinLSHHashing(records);
		
		Vector<Integer> counts = new Vector<Integer>();
		for(int i = 0; i<rlt.size(); i++){
			counts.add(rlt.get(i).size());
		}
		
		System.out.println("Number of Groups: "+rlt.size());
		
		Collections.sort(counts);
		
		System.out.println(counts);
		
		//Iterator<Vector<String>> itr = rlt.iterator();
		//while(itr.hasNext()){
		//	System.out.println(itr.next());
		//}
	}
}
