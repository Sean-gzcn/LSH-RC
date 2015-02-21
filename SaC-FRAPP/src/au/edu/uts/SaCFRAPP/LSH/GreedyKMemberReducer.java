package au.edu.uts.SaCFRAPP.LSH;

import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import au.edu.uts.SaCFRAPP.preprocess.UserCounters;

public class GreedyKMemberReducer extends Reducer<Text, Text, Text, Text>{

	int K = -1;
	QIDistance dist;
	
	Vector<WritableQIRecord> smallPartitions;
	Text keySmallPartition;
	final int properSize = 5000;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		K = context.getConfiguration().getInt("K", -1);
		dist = new QIDistance();
		
		smallPartitions = new Vector<WritableQIRecord>();
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
			
			smallPartitions.addAll(wrVec);
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
				
				Vector<Cluster> clusterVec = this.betaGreedyAgglomClustering(subGroups.get(i));
				if(clusterVec.get(clusterVec.size()-1).size() < K){
					smallPartitions.addAll(clusterVec.remove(clusterVec.size()-1).getClusterElements());
				}
				this.emitCluster(key, clusterVec, context);
			}
			return;
		}
		
		Vector<Cluster> clusterVec = this.betaGreedyAgglomClustering(wrVec);
		if(clusterVec.get(clusterVec.size()-1).size() < K){
			smallPartitions.addAll(clusterVec.remove(clusterVec.size()-1).getClusterElements());
		}
		this.emitCluster(key, clusterVec, context);
	}
	
	private Vector<Cluster> betaGreedyAgglomClustering(Vector<WritableQIRecord> records){
		
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
		
		if(records.size() >= 1){
			Cluster cluster = new Cluster(records);
			clusterVec.add(cluster);
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
		
		Vector<Cluster> clusterVec =this.betaGreedyAgglomClustering(smallPartitions);
		
		if(clusterVec.get(clusterVec.size()-1).size() < K){
			
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
