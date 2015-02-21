package au.edu.uts.SaCFRAPP.LSH;

import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecursiveMinLSHReducer extends Reducer<Text, Text, Text, Text>{

	int widthOfBand = -1;
	int K = -1;
	double weight = 0.0;
	QIDistance dist;
	
	Vector<Cluster> smClusters;
	String lastKey;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		
		K = context.getConfiguration().getInt("K", -1);
		widthOfBand = context.getConfiguration().getInt("WIDTH-LOCAL", -1);
		weight = context.getConfiguration().getFloat("WEIGHT", 0.0f);
		dist = new QIDistance();
		
		smClusters = new Vector<Cluster>();
		lastKey = "";
	}
	
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		context.getCounter(GroupCounters.ORIGINAL_GROUPS).increment(1);
		
		int count = 0;
		
		Vector<String> records = new Vector<String>();
		for(Iterator<Text> i=values.iterator(); i.hasNext(); ){
			records.add(i.next().toString().trim());
		}
		
		if(records.size() < K){
			context.getCounter(GroupCounters.SMALL_GROUPS).increment(1);
			
			Cluster sCluster = new Cluster();
			for(Iterator<String> i = records.iterator(); i.hasNext(); ){
				sCluster.add(i.next());
			}
			smClusters.add(sCluster);
		}
		else if(records.size() == K){
			Cluster rltCluster = new Cluster();
			for(Iterator<String> i = records.iterator(); i.hasNext(); ){
				rltCluster.add(i.next());
			}
			this.emitCluster(context, key, count, rltCluster);
			count++;
		}
		else{
			context.getCounter(GroupCounters.LARGE_GROUPS).increment(1);
			
			Vector<Cluster> rltClusters = this.recursiveClustering(records);
			if(rltClusters.get(rltClusters.size()-1).size() < K){
				Cluster remainingCluster = rltClusters.remove(rltClusters.size()-1);
				smClusters.add(remainingCluster);
			}
			
			for(Iterator<Cluster> i = rltClusters.iterator(); i.hasNext(); ){
				this.emitCluster(context, key, count, i.next());
				count++;
			}
		}
		
		lastKey = key.toString()+count;
		
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

	public void emitCluster(Context context, Text key, int count, Cluster cluster) throws IOException, InterruptedException {
		
		for(Iterator<WritableQIRecord> i= cluster.getClusterElements().iterator(); i.hasNext(); ){
			context.write(new Text(key.toString()+"-"+count), new Text(i.next().toString()));
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		
		Vector<Cluster> rltClusters = this.kmClustering(smClusters);
		
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
		
		int count = 0;
		Text key = new Text(lastKey); 
		for(Iterator<Cluster> i = rltClusters.iterator(); i.hasNext(); ){
			this.emitCluster(context, key, count, i.next());
			count++;
		}
	}
}
