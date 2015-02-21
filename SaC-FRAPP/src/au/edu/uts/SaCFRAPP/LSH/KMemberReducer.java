package au.edu.uts.SaCFRAPP.LSH;

import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import au.edu.uts.SaCFRAPP.preprocess.UserCounters;

public class KMemberReducer extends Reducer<Text, Text, Text, Text>{

	int K = -1;
	QIDistance dist;
	
	Vector<Cluster> smallPartitions;
	Text keySmallPartition;
	final int properSize = 1000;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		K = context.getConfiguration().getInt("K", -1);
		dist = new QIDistance();
		
		smallPartitions = new Vector<Cluster>();
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// In order to count the number of groups
		context.getCounter(GroupCounters.ORIGINAL_GROUPS).increment(1);
		
		Vector<WritableQIRecord> wrVec = new Vector<WritableQIRecord>();
		Iterator<Text> itr = values.iterator();
		while(itr.hasNext()){
			wrVec.add(new WritableQIRecord(itr.next().toString()));
		}
		
		if(wrVec.size() < K){ // Small partitions
			context.getCounter(GroupCounters.SMALL_GROUPS).increment(1);
			
			Cluster smallCluster = new Cluster(wrVec);
			smallPartitions.add(smallCluster);
			keySmallPartition = key;
			
			return;
		}
		
		if(wrVec.size() > properSize){
			context.getCounter(GroupCounters.LARGE_GROUPS).increment(1);
			
			int numOfSubGroups = (int)(wrVec.size()/properSize);
			Vector<Vector<WritableQIRecord>> subGroups = new Vector<Vector<WritableQIRecord>>();
			for(int i = 0; i<numOfSubGroups; i++){
				subGroups.add(new Vector<WritableQIRecord>());
			}
			for(int i = 0; i<wrVec.size(); i++){
				subGroups.get(i%numOfSubGroups).add(wrVec.get(i));
			}
			for(int i = 0; i<subGroups.size(); i++){
				
				Vector<Cluster> clusterVec = this.betaAgglomClustering(subGroups.get(i));
				if(!clusterVec.get(clusterVec.size()-1).isKAnonymous()){
					smallPartitions.add(clusterVec.remove(clusterVec.size()-1));
				}
				this.emitCluster(key, clusterVec, context);
			}
			return;
		}
		
		Vector<Cluster> clusterVec = this.betaAgglomClustering(wrVec);
		if(!clusterVec.get(clusterVec.size()-1).isKAnonymous()){
			smallPartitions.add(clusterVec.remove(clusterVec.size()-1));
		}
		this.emitCluster(key, clusterVec, context);
	}
	
	private Vector<Cluster> gamaAgglomClustering(Vector<Cluster> rawClusters){
		
		// Initialize each record as a cluster
		Cluster[] clusters = new Cluster[rawClusters.size()];
		for(int i = 0; i<clusters.length; i++){
			clusters[i] = rawClusters.get(i);
		}
		
		// Compute distances between clusters
		PriorityQueue<ClusterDistancePair> pQueue = new PriorityQueue<ClusterDistancePair>();
		for(int i = 0; i<clusters.length-1; i++){
			for(int j = i+1; j<clusters.length; j++){
				double clusterDist = dist.clusterDistance(clusters[i],	clusters[j]);
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
						double clusterDist = dist.clusterDistance(clusters[i], clusters[cDPair.clusterX]);
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
	
	private Vector<Cluster> betaAgglomClustering(Vector<WritableQIRecord> wrVec){
		
		// Initialize each record as a cluster
		Vector<Cluster> rawClusters = new Vector<Cluster>();
		
		for(int i = 0; i<wrVec.size(); i++){
			rawClusters.add(new Cluster(wrVec.get(i)));
		}
		
		return this.gamaAgglomClustering(rawClusters);

	}
	
	private void emitCluster(Text key, Vector<Cluster> clusterVec, Context context) throws IOException, InterruptedException{
		for(int i = 0; i<clusterVec.size(); i++){
			Cluster cluster = clusterVec.get(i);
			if(cluster.size() > 2*K-1){
				context.getCounter(GroupCounters.WRONG_CLUSTERS_LARGE).increment(1);
				System.out.println("Error: Wrong cluster large size!!! "+cluster.size());
			}
			if(cluster.size() < K){
				context.getCounter(GroupCounters.WRONG_CLUSTERS_SMALL).increment(1);
				System.out.println("Error: Wrong cluster small size!!! "+cluster.size());
			}
			Iterator<WritableQIRecord> wrItr = cluster.getClusterElements().iterator();
			while(wrItr.hasNext()){
				context.write(new Text(key+"-"+i), new Text(wrItr.next().toString()));
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		
		Vector<Cluster> clusterVec =this.gamaAgglomClustering(smallPartitions);
		
		if(!clusterVec.get(clusterVec.size()-1).isKAnonymous()){
			
			Cluster remainingCluster = clusterVec.remove(clusterVec.size()-1);
			double tmpMin = Double.MAX_VALUE;
			int minIndex = -1;
			for(int i = 0; i<clusterVec.size(); i++){
				double clusterDist = dist.clusterDistance(clusterVec.get(i), remainingCluster);
				if(clusterDist < tmpMin){
					tmpMin = clusterDist;
					minIndex = i;
				}
			}
			if(minIndex == -1){
				System.out.println("Error!!! The clusterVec size is: "+clusterVec.size());
			}
			clusterVec.get(minIndex).mergeWithCluster(remainingCluster);
			if(clusterVec.get(minIndex).size() > 2*K-1){
				Cluster oversizeCluster = clusterVec.remove(minIndex);
				Vector<WritableQIRecord> wrVec = oversizeCluster.clusterElements;
				Vector<WritableQIRecord> wrVec1= new Vector<WritableQIRecord>();
				Vector<WritableQIRecord> wrVec2= new Vector<WritableQIRecord>();
				for(int i = 0; i<wrVec.size()/2; i++){
					wrVec1.add(wrVec.get(i));
				}
				for(int i = wrVec.size()/2; i<wrVec.size(); i++){
					wrVec2.add(wrVec.get(i));
				}
				clusterVec.add(new Cluster(wrVec1));
				clusterVec.add(new Cluster(wrVec2));
			}
		}
		
		this.emitCluster(keySmallPartition, clusterVec, context);
	}
}
