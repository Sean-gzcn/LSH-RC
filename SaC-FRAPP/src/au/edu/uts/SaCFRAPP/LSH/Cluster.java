package au.edu.uts.SaCFRAPP.LSH;

import java.util.Vector;

public class Cluster {

	Vector<WritableQIRecord> clusterElements = new Vector<WritableQIRecord>();
	boolean kAnonymous = false;
	
	public Cluster(){
		
	}
	
	@Override
	public String toString(){
		String result= "";
		for(int i =0; i<clusterElements.size(); i++){
			result += clusterElements.get(i)+"\n";
		}
		return result;
	}
	
	public Cluster(WritableQIRecord wr){
		this.clusterElements.add(wr);
	}
	
	public Cluster(Vector<WritableQIRecord> wrVec){
		this.clusterElements.addAll(wrVec);
	}
	
	public void add(String wrStr){
		this.clusterElements.add(new WritableQIRecord(wrStr));
	}
	
	public Vector<WritableQIRecord> getClusterElements(){
		return this.clusterElements;
	}
	
	public int size(){
		return clusterElements.size();
	}
	
	public void setKAnonymous(boolean kAnonymous){
		this.kAnonymous = kAnonymous;
	}
	
	public boolean isKAnonymous(){
		return kAnonymous;
	}
	
	public void mergeWithCluster(Cluster cluster){
		this.clusterElements.addAll(cluster.getClusterElements());
	}
}
