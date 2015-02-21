package au.edu.uts.SaCFRAPP.LSH;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;


public class GreedyKMemberClustering extends KMemberClustering implements Experiment{

	public GreedyKMemberClustering() {
		super();
	}

	@Override
	public Vector<Cluster> kmClustering(String input) {
		
		Vector<WritableQIRecord> records = this.readDataRecords(input);
		Vector<Cluster> clusterVec = new Vector<Cluster>();
		
		if(records.size() < K){
			System.err.println("Warning: The data set is too small to be k-anonymized!");
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
				double tmpDist = dist.clusterDistance(cluster, clusterVec.get(i));
				if(tmpDist < minDist){
					minDist = tmpDist;
					minIndex = i;
				}
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
	
	@Override
	public void run(String[] args) {
		String input = args[0];
		String output = args[1];
		int K = Integer.valueOf(args[2]);
		
		int numOfRounds = Integer.valueOf(args[3]);
		
		double[] time = new double[numOfRounds];
		
		for(int i = 0; i<numOfRounds; i++){
			
			long start = System.nanoTime();
			this.clustering(K, input+"-"+i, output+"-"+i);
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
		
		//long start = System.nanoTime();
		
		//KMemberClustering kmc = new GreedyKMemberClustering();
		//kmc.clustering(10, "dat/LSH/uniformed_1M.data", "dat/LSH/cluster_1M");
		//System.out.println();
		
		//System.out.println("Clustering finished!\nTotal Time: "+(System.nanoTime()-start)/1000000000.0);

		Experiment exp = new GreedyKMemberClustering();
		
		exp.run(args);
	}

	

}
