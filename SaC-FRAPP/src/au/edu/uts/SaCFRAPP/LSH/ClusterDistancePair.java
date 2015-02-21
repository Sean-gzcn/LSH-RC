package au.edu.uts.SaCFRAPP.LSH;

import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Vector;

public class ClusterDistancePair implements Comparable<ClusterDistancePair>{

	int clusterX;
	int clusterY;
	double distance;
	
	public ClusterDistancePair(){
		
	}
	
	public ClusterDistancePair(int clusterX, int clusterY, double distance){
		this.clusterX = clusterX;
		this.clusterY = clusterY;
		this.distance = distance;
	}
	
	public int getClusterX(){
		return clusterX;
	}
	
	public int getClusterY(){
		return clusterY;
	}
	
	public double getDistance(){
		return distance;
	}
	
	@Override
	public String toString(){
		return "[<"+this.clusterX+", "+this.clusterY+">, "+this.distance+"]";		
	}

	@Override
	public int compareTo(ClusterDistancePair cDPair) {
		if(this.distance > cDPair.getDistance()){
			return 1;
		}
		else if(this.distance < cDPair.getDistance()){
			return -1;
		}else{
			return 0;
		}
	}
	
	@Override
	public boolean equals(Object obj){
		
		if(obj == null){
			return false;
		}
		if(obj == this){
			return true;
		}
		if(!(obj instanceof ClusterDistancePair)) {
			return false;
		}
		
		ClusterDistancePair cDPair = (ClusterDistancePair)obj;
		if(this.clusterX == cDPair.getClusterX() || this.clusterY == cDPair.getClusterY() || 
				this.clusterX == cDPair.getClusterY() || this.clusterY == cDPair.getClusterX()){
			return true;
		}
		return false;
	}
	
	public static void main(String[] args){
		PriorityQueue<ClusterDistancePair> pq = new PriorityQueue<ClusterDistancePair>();
		pq.add(new ClusterDistancePair(1, 2, 3.0));
		pq.add(new ClusterDistancePair(1, 3, 7.0));
		pq.add(new ClusterDistancePair(2, 3, 5.0));
		pq.add(new ClusterDistancePair(4, 5, 10.0));
		
		ClusterDistancePair cdp1;
		System.out.println(cdp1 = pq.poll());
		System.out.println(pq.toString());
		
		pq.remove(cdp1);
		//pq.remove(cdp1);
		//pq.remove(cdp1);
		System.out.println(pq.toString());
		Iterator<ClusterDistancePair> itr = pq.iterator();
		while(itr.hasNext()){
			if(cdp1.equals(itr.next())){
				itr.remove();
			}
		}
		System.out.println(pq.toString());
	}
}
