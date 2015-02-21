package au.edu.uts.SaCFRAPP.LSH;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Vector;

public class AggloKMemberClustering extends KMemberClustering implements Experiment{

	double weight; // To adjust distance between two clusters
	
	public AggloKMemberClustering() {
		super();
		
		weight = 0.1;
	}
	
	public void setWeight(double weight){
		this.weight = weight;
	}
	
	public double getWeight(){
		return weight;
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
		
		Vector<Cluster> rawClusters = new Vector<Cluster>();
		for(int i = 0; i<records.size(); i++){
			rawClusters.add(new Cluster(records.get(i)));
		}
		
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


	public Vector<Cluster> kmClustering(Vector<Cluster> rawClusters){
		
		// Initialize each record as a cluster
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
					System.out.println("One non-k-anonymous cluster remains!!!");
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
		
		int numOfRounds = Integer.valueOf(args[3]);
		
		double baseWeight = 0.05;
		double weightStep = 0.5/numOfRounds;
		
		double[] time = new double[numOfRounds];
		
		for(int i = 0; i<numOfRounds; i++){
			
			long start = System.nanoTime();
			this.setWeight(baseWeight+weightStep*i);
			this.clustering(K, input, output+"-"+i);
			System.out.println("The "+i+"th round of Clustering finished!");
			time[i] = (System.nanoTime()-start)/1000000000.0;
		}
		
		try{
			PrintWriter pw = new PrintWriter(new FileOutputStream(new File(output+"-time")));
			for(int i = 0; i<time.length; i++){
				System.out.println(time[i]);
				pw.println(time[i]);
			}
			pw.flush();
			pw.close();
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
		
		AggloKMemberClustering kmc = new AggloKMemberClustering();
		kmc.setWeight(0.1);
		kmc.clustering(10, "dat/LSH/uniformed_1M.data", "dat/LSH/cluster_1M_agg");
		System.out.println();
		
		System.out.println("Clustering finished!\nTotal Time: "+(System.nanoTime()-start)/1000000000.0);
		*/
		
		Experiment exp = new AggloKMemberClustering();
		exp.run(args);
	}

}
