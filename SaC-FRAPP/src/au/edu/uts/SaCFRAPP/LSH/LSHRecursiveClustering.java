package au.edu.uts.SaCFRAPP.LSH;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Vector;

public class LSHRecursiveClustering extends KMemberClustering implements Experiment{
	
	double weight;
	int widthOfBand;
	
	public LSHRecursiveClustering() {
		super();
		
		weight = 0.1;
		widthOfBand = 2;
	}
	
	public void setWeight(double weight){
		this.weight = weight;
	}
	
	public double getWeight(){
		return weight;
	}
	
	public void setWidthOfBand(int widthOfBand){
		this.widthOfBand = widthOfBand;
	}
	
	public int getWidthOfBand(){
		return this.widthOfBand;
	}
	
	@Override
	protected Vector<Cluster> kmClustering(String input) {
		
		Vector<WritableQIRecord> records = this.readDataRecords(input);
		
		if(records.size() < K){
			System.err.println("Warning: The data set is too small to be k-anonymized!");
			Vector<Cluster> clusterVec = new Vector<Cluster>();
			clusterVec.add(new Cluster(records));
			return clusterVec;
		}
		
		Vector<String> rawRecords = new Vector<String>();
		for(int i = 0; i<records.size(); i++){
			rawRecords.add(records.get(i).toString());
		}
		
		Vector<Cluster> rltClusters = this.recursiveClustering(rawRecords);
		
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

	
	public Vector<Cluster> recursiveClustering(Vector<String> records){
		
		Vector<Cluster> rltClusters = new Vector<Cluster>();
		
		if(records.size() <= K){
			Cluster remainingCluster = new Cluster();
			for(int i = 0; i<records.size(); i++){
				remainingCluster.add(records.get(i));
			}
			rltClusters.add(remainingCluster);
			return rltClusters;
		}
		
		SingleBandMinLSH sbms = new SingleBandMinLSH(widthOfBand);
		Vector<Vector<String>> buckets = sbms.hashing(records);
	/*Vector<Integer> counts = new Vector<Integer>();
	for(int i = 0; i<buckets.size(); i++){
		counts.add(buckets.get(i).size());
	}
	System.out.println("Number of Groups: "+buckets.size());
	Collections.sort(counts);	
	System.out.println(counts);*/
		
		// Build small clusters
		Vector<Cluster> smallClusters = new Vector<Cluster>();
		
		for(int i=0; i<buckets.size(); i++){
			if(buckets.get(i).size() <= K){
				Cluster sCluster = new Cluster();
				for(int j = 0; j<buckets.get(i).size(); j++){
					sCluster.add(buckets.get(i).get(j));
				}
				smallClusters.add(sCluster);
			}
			else{
				// Recursively handle large buckets
				Vector<Cluster> tmpClusters = recursiveClustering(buckets.get(i));
				
				int tmpIndex = -1;
				if(tmpClusters.get(tmpClusters.size()-1).size() < K){  // Remaining cluster
					tmpIndex = tmpClusters.size()-2;
					smallClusters.add(tmpClusters.get(tmpClusters.size()-1));
				}
				else{
					tmpIndex = tmpClusters.size()-1;
				}
				
				// Put already clustered clusters in the result vector
				for(; tmpIndex >= 0; tmpIndex--){
					rltClusters.add(tmpClusters.get(tmpIndex));
				}
			}
		}
		
		// Clustering small clusters
		Vector<Cluster> tmpClusters = this.kmClustering(smallClusters);
		for(int i = 0; i<tmpClusters.size(); i++){
			rltClusters.add(tmpClusters.get(i));
		}
		
		return rltClusters;
	}
	
	private Vector<Cluster> kmClustering(Vector<Cluster> rawClusters){
		
		// Initialize
		Cluster[] clusters = new Cluster[rawClusters.size()];
		for(int i = 0; i<clusters.length; i++){
			clusters[i] = rawClusters.get(i);
		}
		
		// Compute distances between clusters
		PriorityQueue<ClusterDistancePair> pQueue = new PriorityQueue<ClusterDistancePair>();
		for(int i = 0; i<clusters.length-1; i++){
			for(int j = i+1; j<clusters.length; j++){
				//double clusterDist = dist.clusterDistance(clusters[i],	clusters[j]);
				double clusterDist = dist.clusterDistance(clusters[i],	clusters[j], K, weight);
				pQueue.add(new ClusterDistancePair(i, j, clusterDist));
			}
		}
								
		while(!pQueue.isEmpty()){
					
			// Merge two clusters with the shortest distance into a new cluster
			ClusterDistancePair cDPair = pQueue.poll();
			clusters[cDPair.clusterX].mergeWithCluster(clusters[cDPair.clusterY]);
			clusters[cDPair.clusterY] = null;
									
			// Check whether is k anonymous
			if(clusters[cDPair.clusterX].size() >= K){
				clusters[cDPair.clusterX].setKAnonymous(true);
			}
									
			// Remove distance entries related to old clusters
			Iterator<ClusterDistancePair> itrPQ = pQueue.iterator();
			while(itrPQ.hasNext()){
				if(itrPQ.next().equals(cDPair)){
					itrPQ.remove();
				}
			}
							
			// Insert new distance entries for the new cluster
			if(!clusters[cDPair.clusterX].kAnonymous){
				for(int i = 0; i<clusters.length; i++){
					if(clusters[i] != null && (!clusters[i].isKAnonymous()) && i!= cDPair.clusterX){
						//double clusterDist = dist.clusterDistance(clusters[i], clusters[cDPair.clusterX]);
						double clusterDist = dist.clusterDistance(clusters[i], clusters[cDPair.clusterX], K, weight);
						pQueue.add(new ClusterDistancePair(cDPair.clusterX, i, clusterDist));
					}
				}
			}
		}
			
		Vector<Cluster> clusterVec = new Vector<Cluster>();
		Cluster remainingCluster = null;
		int remainingCounter = 0;
		for(int i = 0; i<clusters.length; i++){
			if(clusters[i] != null){
				if(clusters[i].isKAnonymous()){
					clusterVec.add(clusters[i]);
				}
				else{
					//System.out.println("One non-k-anonymous cluster remains!!!");
					remainingCounter++;
					remainingCluster = clusters[i];
				}				
			}
		}
		
		if(remainingCounter > 1){
			System.out.println("Error!!! More than one remaining clusters exist!");
		}
		if(remainingCluster != null){
			clusterVec.add(remainingCluster);
		}
		
		return clusterVec;
	}
	
	@Override
	public void run(String[] args) {
		
		String input = args[0];
		String output = args[1];
		int K = Integer.valueOf(args[2]);
		
		int numOfDatasets = Integer.valueOf(args[3]);
		int numOfGama = Integer.valueOf(args[4]);
		
		double[][] time = new double[numOfGama][numOfDatasets];
		
		try{
			PrintWriter pw = new PrintWriter(new FileOutputStream(new File(output+"-time")));
			
			this.setWeight(0.1);
			
			for(int i = 0; i<numOfGama; i++){
				
				this.setWidthOfBand(i+2);
				
				for(int j = 0; j<numOfDatasets; j++){
					
					long start = System.nanoTime();
					
					this.clustering(K, input+"-"+j, output+"-"+i+"-"+j);
					
					System.out.println("Clustering the "+j+"th dataset with gama "+(i+2)+" finished!");
					time[i][j] = (System.nanoTime()-start)/1000000000.0;
					pw.println(time[i][j]);
					pw.flush();
				}
				System.out.println();
			}
			pw.close();
			
			for(int i = 0; i<numOfGama; i++){
				for(int j = 0; j<numOfDatasets; j++){
					System.out.print(time[i][j]+"\t");
				}
				System.out.println();
				
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		/*
		long start = System.nanoTime();
		
		LSHRecursiveClustering kmc = new LSHRecursiveClustering();
		
		//kmc.setWeight(0.1);
		kmc.setWeight(Double.valueOf(args[0].trim()));
		
		//kmc.setWidthOfBand(2);
		kmc.setWidthOfBand(Integer.valueOf(args[1].trim()));
		
		//kmc.clustering(10, "dat/LSH/uniformed_100M.data", "dat/LSH/cluster_100M_R");
		kmc.clustering(Integer.valueOf(args[2].trim()), args[3].trim(), args[4].trim());
		
		System.out.println("Clustering finished!\nTotal Time: "+(System.nanoTime()-start)/1000000000.0);
		*/
		
		Experiment exp = new LSHRecursiveClustering();
		//String[] argv = {"dat/LSH/uniformed_100K", "dat/LSH/cluster_100K", "10"};
		exp.run(args);
	}

	

}
