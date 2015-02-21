package au.edu.uts.SaCFRAPP.LSH;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

public class SingleBandMinLSH {

	MinHash minHash;
	int numOfHashFunctions;
	int widthOfBand;
	int[][] hashFamily;
	HashMap<String, Integer> IDBucketIndex;
	Vector<Vector<String>> rawBuckets;
	
	public SingleBandMinLSH(int widthOfBand) {

		minHash = new MinHash();
		this.widthOfBand = widthOfBand;
		numOfHashFunctions = this.widthOfBand;
		hashFamily = new int[numOfHashFunctions][3];
		
		Random r = new Random();
		for(int i = 0; i<numOfHashFunctions; i++){
			hashFamily[i][0] = r.nextInt();
			hashFamily[i][1] = r.nextInt();
			hashFamily[i][2] = r.nextInt();
		}
		
		IDBucketIndex = new HashMap<String, Integer>();  // Indexing for bucket by groupID
		rawBuckets = new Vector<Vector<String>>();
	}
	
	public Vector<Vector<String>> hashing(Vector<String> records){
		Iterator<String> recordItr = records.iterator();
		String record;
		while(recordItr.hasNext()){
			record = recordItr.next();
			
			String bucketID = "";
			int j = 0;
			for(; j<widthOfBand-1; j++){
				int mHash = minHash.minHash(record, hashFamily[j][0], 
						hashFamily[j][1], hashFamily[j][2]);
				bucketID = bucketID+mHash+"-";
			}
			int mHash = minHash.minHash(record, hashFamily[j][0], 
					hashFamily[j][1], hashFamily[j][2]);
			bucketID = bucketID+mHash;
			
			int bucketIndex = this.getBucketIndex(bucketID);
			rawBuckets.get(bucketIndex).add(record);
		}
		
		return rawBuckets;
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
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Vector<String> records = new Vector<String>();
		try{
			String dataPathStr = "dat/LSH/uniformed_100k.data";
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
		
		SingleBandMinLSH sms = new SingleBandMinLSH(2);
		Vector<Vector<String>> rlt = sms.hashing(records);
		
		Vector<Integer> counts = new Vector<Integer>();
		for(int i = 0; i<rlt.size(); i++){
			counts.add(rlt.get(i).size());
		}
		
		System.out.println("Number of Groups: "+rlt.size());
		
		Collections.sort(counts);
		
		System.out.println(counts);

	}

}
