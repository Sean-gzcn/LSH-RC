package au.edu.uts.SaCFRAPP.LSH;

import java.util.Iterator;
import java.util.Vector;

import au.edu.uts.SaCFRAPP.common.Attribute;
import au.edu.uts.SaCFRAPP.common.Common;

public class QIDistance {

	Vector<Double> weightVecQI = new Vector<Double>();
	double[] bounds = new double[Common.NUM_ATTR];
	
	public QIDistance(){
		
		for(int i = 0; i<Common.NUM_ATTR; i++){
			weightVecQI.add(1.0/Common.NUM_ATTR);
		}
		
		Attribute[] attributes = Attribute.values();
		for(int i=0; i<Common.NUM_ATTR; i++){
			bounds[i] = 2*Common.taxonomyForest.getHeight(attributes[i].ordinal());
		}
	}
	
	public void printWeights(){
		System.out.println("Weights for each QI attribute: \n"+weightVecQI.toString());		
	}
	
	public void printBounds(){
		for(int i = 0; i<bounds.length-1; i++){
			System.out.print(bounds[i]+", ");
		}
		System.out.print(bounds[bounds.length-1]+"\n");
	}
	
	public void setQIWeights(Vector<Double> weightVecQI){
		this.weightVecQI = weightVecQI;
	}
	
	public double getQIWeight(int index){
		return weightVecQI.get(index);
	}
	
	public double getBound(int index){
		return this.bounds[index];
	}
	
	public double distanceQI(WritableQIRecord recordx, WritableQIRecord recordy){
		
		double dist = 0.0;
		for(int i=0; i<Common.NUM_ATTR; i++){
			double tmpDist = Common.taxonomyForest.distance(i, 
					recordx.getCategoricalQI().get(i), 
					recordy.getCategoricalQI().get(i));
			if(tmpDist < 0){
				System.out.println("Error: No distance!!! ("+recordx.getCategoricalQI().get(i)+", "+
						recordy.getCategoricalQI().get(i)+")");
				return -1;
			}
			dist += weightVecQI.get(i)*tmpDist/bounds[i];
		}
		return dist;
	}
	
	public double clusterDistance(Cluster clusterX, Cluster clusterY){
		
		// Find the largest distance between records in two clusters 
		double maxDist = Double.MIN_VALUE;
		Iterator<WritableQIRecord> itrX = clusterX.getClusterElements().iterator();
		while(itrX.hasNext()){
			WritableQIRecord wrX = itrX.next();
			Iterator<WritableQIRecord> itrY = clusterY.getClusterElements().iterator();
			while(itrY.hasNext()){
				WritableQIRecord wrY = itrY.next();
				double tmpDist = this.distanceQI(wrX, wrY);
				if(tmpDist > maxDist){
					maxDist = tmpDist;
				}
			}
			
		}
		assert maxDist != Double.MIN_VALUE : "maxDist has not changed!";
		return maxDist*(clusterX.size()+clusterY.size());
	}
	
	public double clusterDistance(Cluster clusterX, Cluster clusterY, int K, double weight){
		
		// Find the largest distance between records in two clusters 
		double maxDist = Double.MIN_VALUE;
		Iterator<WritableQIRecord> itrX = clusterX.getClusterElements().iterator();
		while(itrX.hasNext()){
			WritableQIRecord wrX = itrX.next();
			Iterator<WritableQIRecord> itrY = clusterY.getClusterElements().iterator();
			while(itrY.hasNext()){
				WritableQIRecord wrY = itrY.next();
				double tmpDist = this.distanceQI(wrX, wrY);
				if(tmpDist > maxDist){
					maxDist = tmpDist;
				}
			}
			
		}
		assert maxDist != Double.MIN_VALUE : "maxDist has not changed!";
		//return maxDist*(1+weight*Math.abs(clusterX.size()+clusterY.size()-K));
		
		double index = 1.0;
		if(clusterX.size()+clusterY.size() <= K){
			index = 1+weight*(K-(clusterX.size()+clusterY.size()));
		}
		else{
			index = 1+weight*Math.pow(clusterX.size()+clusterY.size()-K, 3);
		}
		return maxDist*index;
	}
	
	public static void main(String args[]){
		
		QIDistance dist = new QIDistance();
		dist.printWeights();

		WritableQIRecord recordx = new WritableQIRecord("45, 12th, Married-civ-spouse, Sales, Other-relative, White, Female, Italy, Private");
		WritableQIRecord recordy = new WritableQIRecord("82, Assoc-acdm, Married-AF-spouse, Transport-moving, Other-relative, Black, Female, Germany, SelfEmpNotInc");
		System.out.println("Record x: \n"+recordx);
		System.out.println("Record y: \n"+recordy);
		System.out.println("The QI distance: "+dist.distanceQI(recordx, recordy));
		
		
		Cluster clusterx = new Cluster(recordx);
		clusterx.add("29, Assoc-voc, Married-civ-spouse, Tech-support, Own-child, Other, Female, Yugoslavia, SelfEmpNotInc");
		Cluster clustery = new Cluster(recordy);
		System.out.println("Cluster x: \n"+clusterx);
		System.out.println("Cluster y: \n"+clustery);
		System.out.println("The QI distance: "+dist.clusterDistance(clusterx, clustery));
	} 
}
