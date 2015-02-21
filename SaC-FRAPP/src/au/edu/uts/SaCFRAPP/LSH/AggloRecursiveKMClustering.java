package au.edu.uts.SaCFRAPP.LSH;

import java.util.Iterator;
import java.util.Vector;

public class AggloRecursiveKMClustering extends AggloKMemberClustering {

	Vector<Cluster> rawClusters;
	int widthOfBand;
	
	public AggloRecursiveKMClustering() {
		super();
		
		rawClusters = new Vector<Cluster>();
		widthOfBand = 2;
	}
	
	public void setWidthOfBand(int widthOfBand){
		this.widthOfBand = widthOfBand;
	}
	
	public int getWidthOfBand(){
		return this.widthOfBand;
	}
	
	@Override
	protected Vector<Cluster> kmClustering(String input) {
		
		//Vector<WritableQIRecord> records = this.readDataRecords(input);
		Vector<String> records = this.readDataStrings(input);
		
		if(records.size() < K){
			System.err.println("Warning: The data set is too small to be k-anonymized!");
			Vector<Cluster> clusterVec = new Vector<Cluster>();
			Cluster rltCluster = new Cluster();
			for(int i = 0; i<records.size(); i++){
				rltCluster.add(records.get(i));
			}
			clusterVec.add(rltCluster);
			return clusterVec;
		}
		
		this.recursivePartitioning(records);
		
		System.out.println("After partitioning!");
		System.out.println("The number of raw clusters: "+rawClusters.size());
		System.out.println("The number of records: "+records.size());
		
		Vector<Cluster> rltClusters =this.kmClustering(rawClusters);
		
		//Handle the remaining cluster
		if(rltClusters.get(rltClusters.size()-1).size() < K){
			Cluster remainingCluster = rltClusters.remove(rltClusters.size()-1);
			Vector<WritableQIRecord> remainingRecords = remainingCluster.clusterElements;
			Iterator<WritableQIRecord> wrItr = remainingRecords.iterator();
			while(wrItr.hasNext()){
				Cluster cluster = new Cluster(wrItr.next());
				double minDist = Double.MAX_VALUE;
				int minIndex = -1;
				for(int i = 0; i<rltClusters.size(); i++){
					double tmpDist = dist.clusterDistance(cluster, rltClusters.get(i));
					if(tmpDist < minDist){
						minDist = tmpDist;
						minIndex = i;
					}
				}
				rltClusters.get(minIndex).mergeWithCluster(cluster);
			}
		}
		
		return rltClusters;
	}
	
	public void recursivePartitioning(Vector<String> records){
		
		SingleBandMinLSH sbms = new SingleBandMinLSH(widthOfBand);
		Vector<Vector<String>> buckets = sbms.hashing(records);
		
		Iterator<Vector<String>> bucItr = buckets.iterator();
		while(bucItr.hasNext()){
			Vector<String> curBucket = bucItr.next();
			if(curBucket.size()<=K){
				Cluster curCluster = new Cluster();
				for(Iterator<String> i = curBucket.iterator(); i.hasNext(); ){
					curCluster.add(i.next());
				}
				rawClusters.add(curCluster);
				bucItr.remove();
			}
		}
		
		for(Iterator<Vector<String>> i = buckets.iterator(); i.hasNext(); ){
			this.recursivePartitioning(i.next());
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		long start = System.nanoTime();
		AggloRecursiveKMClustering kmc = new AggloRecursiveKMClustering();
		kmc.setWeight(0.1);
		kmc.setWidthOfBand(2);
		kmc.clustering(10, "dat/LSH/uniformed_1M.data", "dat/LSH/cluster_1M_RA");
		System.out.println();
		
		System.out.println("Clustering finished!\nTotal Time: "+(System.nanoTime()-start)/1000000000.0);

	}

}
