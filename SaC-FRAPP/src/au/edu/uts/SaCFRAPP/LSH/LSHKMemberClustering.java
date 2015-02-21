package au.edu.uts.SaCFRAPP.LSH;

import java.io.*;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Vector;

public class LSHKMemberClustering {
	
	QIDistance dist;
	
	public LSHKMemberClustering(){
		dist = new QIDistance();
	}
	
	public Vector<WritableQIRecord> readDataRecords(String dataPathStr){
		
		Vector<WritableQIRecord> records = new Vector<WritableQIRecord>();
		try{
			File file = new File(dataPathStr);
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			
			String line;
			while((line = br.readLine()) != null){
				records.add(new WritableQIRecord(line));
			}
			
			br.close();
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
		
		return records;
	}
	
	public void writeClusters(String clusterPathStr, Vector<Cluster> clusters){
		
		try{
			File file = new File(clusterPathStr);
			PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)));
			
			for(int i = 0; i<clusters.size(); i++){
				Iterator<WritableQIRecord> wrItr = clusters.get(i).getClusterElements().iterator();
				while(wrItr.hasNext()){
					pw.append(i+"\t"+wrItr.next().toString()+"\n");
				}
			}
			pw.flush();
			pw.close();
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	public Vector<Cluster> kMember(int K, String dataPathStr){
		return greedyAgglomClustering(K, dataPathStr);
	}
	
	public Vector<Cluster> greedyAgglomClustering(int K, String dataPathStr){
		
		Vector<WritableQIRecord> records = this.readDataRecords(dataPathStr);
		
		Vector<Cluster> clusterVec = new Vector<Cluster>();
		
		if(records.size() <= K){
			clusterVec.add(new Cluster(records));
			return clusterVec;
		}
		
		Random rand = new Random();
		WritableQIRecord wr = records.get(rand.nextInt(records.size()));
		
		while(records.size()>= K){
			wr = this.removeFurthestRecord(wr, records);
			Cluster cluster = new Cluster(wr);
			while(cluster.size() < K){
				cluster.mergeWithCluster(new Cluster(this.removeClosestRecord(cluster, records)));
			}
			cluster.setKAnonymous(true);
			clusterVec.add(cluster);
		}
		
		Iterator<WritableQIRecord> wrItr = records.iterator();
		while(wrItr.hasNext()){
			Cluster cluster = new Cluster(wrItr.next());
			double minDist = Double.MAX_VALUE;
			int minIndex = -1;
			for(int i = 0; i<clusterVec.size(); i++){
				if(clusterVec.get(i).size() < 2*K-1){
					double tmpDist = dist.clusterDistance(cluster, clusterVec.get(i));
					if(tmpDist < minDist){
						minDist = tmpDist;
						minIndex = i;
					}
				}
			}
			if(minIndex == -1){
				System.out.println("Error!!! Extreme case takes place, need special process!");
			}
			clusterVec.get(minIndex).mergeWithCluster(cluster);
		}
		
		return clusterVec;
	}
	
	private WritableQIRecord removeClosestRecord(Cluster cluster, Vector<WritableQIRecord> records){
		double minDist = Double.MAX_VALUE;
		int minIndex = -1;
		
		for(int i = 0; i<records.size(); i++){
			WritableQIRecord tmp = records.get(i);
			double tmpDist = dist.clusterDistance(cluster, new Cluster(tmp));
			if(tmpDist < minDist){
				minDist = tmpDist;
				minIndex = i;
			}
		}
		
		return records.remove(minIndex);
	}
	
	private WritableQIRecord removeFurthestRecord(WritableQIRecord wr, Vector<WritableQIRecord> records){
		double maxDist = Double.MIN_VALUE;
		int maxIndex = -1;
		
		for(int i = 0; i<records.size(); i++){
			WritableQIRecord tmp = records.get(i);
			double tmpDist = dist.distanceQI(wr, tmp);
			if(tmpDist > maxDist){
				maxDist = tmpDist;
				maxIndex = i;
			}
		}
		
		return records.remove(maxIndex);
	}
	
	public Vector<Cluster> agglomClustering(int K, String dataPathStr){
		
		Vector<WritableQIRecord> records = this.readDataRecords(dataPathStr);
		
		// Initialize each record as a cluster
		Cluster[] clusters = new Cluster[records.size()];
		for(int i = 0; i<clusters.length; i++){
			clusters[i] = new Cluster(records.get(i));
		}
				
		// Compute distances between clusters
	//int counter = 0;
		PriorityQueue<ClusterDistancePair> pQueue = new PriorityQueue<ClusterDistancePair>();
		for(int i = 0; i<clusters.length-1; i++){
			for(int j = i+1; j<clusters.length; j++){
				double clusterDist = dist.clusterDistance(clusters[i],	clusters[j]);
			//counter++;
			//System.out.println(counter);
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
			for(int i = 0; i<clusters.length; i++){
				if(clusters[i] != null && (!clusters[i].isKAnonymous()) && i!= cDPair.clusterX){
					double clusterDist = dist.clusterDistance(clusters[i], clusters[cDPair.clusterX]);
					pQueue.add(new ClusterDistancePair(cDPair.clusterX, i, clusterDist));
				}
			}
					
		}
				
		// Process the remaining cluster
		Vector<Cluster> clusterVec = new Vector<Cluster>();
		Cluster remainingCluster = null;
		for(int i = 0; i<clusters.length; i++){
			if(clusters[i] != null){
				if(clusters[i].isKAnonymous()){
					clusterVec.add(clusters[i]);
				}
				else{
					System.out.println("One non-k-anonymous cluster remains!!!");
					remainingCluster = clusters[i];
				}				
			}
		}
		
		if(remainingCluster != null){
			for(int i = 0; i<remainingCluster.size(); i++){
				WritableQIRecord rWR = remainingCluster.getClusterElements().get(i);
				Cluster reCluster = new Cluster(rWR);
				double minDist = Double.MAX_VALUE;
				int minIndex = -1;
				for(int j = 0; j<clusterVec.size(); j++){
					if(clusterVec.get(j).size() < (2*K-1)){
						double tmpDist = dist.clusterDistance(reCluster, clusterVec.get(j));
						if(tmpDist < minDist){
							minDist = tmpDist;
							minIndex = j;
						}
					}
				}
				if(minIndex != -1){
					clusterVec.get(minIndex).mergeWithCluster(reCluster);
				}
				else{
					System.out.println("Warning: The extreme case happens!!!");
					Vector<WritableQIRecord> restVec = new Vector<WritableQIRecord>();
					for(; i<remainingCluster.size(); i++){
						restVec.add(remainingCluster.getClusterElements().get(i));
					}
					int restSize = restVec.size();
					for(int k = 0; k<(K-restSize); k++){
						restVec.add(clusterVec.get(k).getClusterElements().remove(
										clusterVec.get(k).getClusterElements().size()-1));
					}
					clusterVec.add(new Cluster(restVec));
					break;
				}
			}
		}
		
		return clusterVec;
	}
	
	public static void experiment(){
		for(int i = 0; i<10; i++){
			long start = System.nanoTime();
			LSHKMemberClustering kmc = new LSHKMemberClustering();
			int K = 10;
			String dataPathStr = "dat/r_"+i+"0000";
			String clusterPathStr = "rlt/clusters_"+i;
			kmc.writeClusters(clusterPathStr, kmc.kMember(K, dataPathStr));
			System.out.println("Total Time: "+(System.nanoTime()-start)/1000000000.0);
		}
	}

	public static void main(String args[]){
		
		long start = System.nanoTime();
		
		//String dataPathStr = "dat/LSH/uniformed_500K.data";
		String dataPathStr = args[0].trim();
		//String clusterPathStr = "dat/LSH/clusters";
		String clusterPathStr = args[1].trim();
		//int K = 10;
		int K = Integer.valueOf(args[2].trim());
		int type = Integer.valueOf(args[3].trim());
		
		LSHKMemberClustering kmc = new LSHKMemberClustering();
		
		if(type == 1){
			kmc.writeClusters(clusterPathStr, kmc.greedyAgglomClustering(K, dataPathStr));
		}
		else if (type == 2){
			kmc.writeClusters(clusterPathStr, kmc.agglomClustering(K, dataPathStr));
		}
		else{
			System.out.println("Type 1: Greedy");
			System.out.println("Type 2: Optimal");
		}
		
		System.out.println("Total Time: "+(System.nanoTime()-start)/1000000000.0);
		
		//KMemberClustering.experiment();
	}
}
