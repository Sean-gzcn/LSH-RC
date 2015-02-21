package au.edu.uts.SaCFRAPP.datastructure;
import au.edu.uts.SaCFRAPP.common.*;
import au.edu.uts.SaCFRAPP.taxonomy.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.Writable;



public class CutWritable implements Writable {

	private Vector<Pair> cutElements;
	
	public CutWritable(){
		cutElements = new Vector<Pair>();
	}
	
	public void initializeAsLowest(int index){
		//cutElements.addAll(gTree.getAVNodeValuesByLevel(1));
		Iterator<String> itr = Common.taxonomyForest.vectorOfTrees.elementAt(index).getAVNodeValuesByLevel(1).iterator();
		while(itr.hasNext()){
			cutElements.add(new Pair(itr.next()));
		}
	}
	
	public void initializeAsHighest(int index){
		//cutElements.add(gTree.getTopDomain());
		cutElements.add(new Pair(Common.taxonomyForest.vectorOfTrees.elementAt(index).getTopDomain()));
	}
	
	
	public Vector<Pair> getElements(){
		return cutElements;
	}
	
	public String toString(){
		String rlt = "{";
		for(int i = 0; i<cutElements.size()-1; i++){
			rlt += cutElements.elementAt(i)+", ";
		}
		if(cutElements.size() > 0){
			rlt += cutElements.elementAt(cutElements.size()-1);
		}
		rlt += "}";
		
		return rlt;
	}
	
	public void fillIG(HashMap<String, Double> IGMap){
		Iterator<Pair> pairItr = this.cutElements.iterator();
		while(pairItr.hasNext()){
			Pair pair = pairItr.next();
			pair.setInfo(IGMap.get(pair.getGeneralizaiton()));
		}
	}
	
	public void fillAllPG(HashMap<String, Double> pgMap){
		Iterator<Pair> pairItr = this.cutElements.iterator();
		while(pairItr.hasNext()){
			Pair pair = pairItr.next();
			//if(pair.isValid()){
				pair.setAnonymity(pgMap.get(pair.getGeneralizaiton()));
			//}
		}
	}
	
	public boolean contains(String generalization){
		Iterator<Pair> itr = this.cutElements.iterator();
		while(itr.hasNext()){
			Pair pair = itr.next();
			if(pair.getGeneralizaiton().equals(generalization)){
				return true;
			}
		}
		return false;
	}
	
	private boolean isTop(String generalization){
		boolean flag = false;
		for(int i = 0; i<Common.NUM_ATTR; i++){
			if(generalization.equals(Common.TOP_VALUE[i])){
				flag = true;
			}
		}
		return flag;
	}
	
	
	/*
	public boolean setInvalid(String generalization, int index){
		for(int i = 0; i<this.cutElements.size(); i++){
			Pair pair = this.cutElements.elementAt(i);
			if(pair.getGeneralizaiton().equals(generalization)){
				pair.setValidity(false);
				break;
			}
		}
		h
		boolean needRecompute;
		
		if(this.isTop(generalization)){
			
			needRecompute = false;
			// Adjust cut
			Iterator<Pair> itrOfPair = this.cutElements.iterator();
			while(itrOfPair.hasNext()){
				Pair pair = itrOfPair.next();
				if(pair.getGeneralizaiton().equals(generalization)){
					itrOfPair.remove();
				}
			}
			if(this.cutElements.size() > 0){
				System.out.println("Error: The size of cut should be zero!");
			}
		}
		else{
			//needRecompute = true;
			needRecompute = false;
			int flagCount = 0;
			String parent = Common.taxonomyForest.vectorOfTrees.elementAt(index).getParentValue(generalization);
			Vector<String> children = Common.taxonomyForest.vectorOfTrees.elementAt(index).getChildValues(parent);
			Iterator<String> itr = children.iterator();
			while(itr.hasNext()){
				String child = itr.next();
				Iterator<Pair> itrOfPair = this.cutElements.iterator();
				while(itrOfPair.hasNext()){
					Pair pair = itrOfPair.next();
					if(child.equals(pair.getGeneralizaiton()) && (!pair.isValid())){
						flagCount++;
						break;
					}
				}
			}
			if(flagCount == children.size()){
				needRecompute = true;
			}
			
			if(needRecompute){
				// Adjust cut
				Iterator<Pair> itrOfPair = this.cutElements.iterator();
				while(itrOfPair.hasNext()){
					Pair pair = itrOfPair.next();
					if(children.contains(pair.getGeneralizaiton())){
						itrOfPair.remove();
					}
				}
				
				Pair pPair = new Pair(parent);
				this.cutElements.add(pPair);
			}
		}

		return needRecompute;
	}
	
	*/
	
	/*
	public boolean isValid(String generalization){
		boolean flag = false;
		Iterator<Pair> itr = this.cutElements.iterator();
		while(itr.hasNext()){
			Pair pair = itr.next();
			if(pair.getGeneralizaiton().equals(generalization) && pair.isValid()){
				flag = true;
				break;
			}
		}
		return flag;
	}
	*/
	
	public Pair getNonFilledGen(){
		Iterator<Pair> itr = this.cutElements.iterator();
		while(itr.hasNext()){
			Pair pair = itr.next();
			if(pair.getInfo() < 0){
				return pair;
			}
		}
		return null;
	}
	
	/*
	public boolean setKAnonymized(String parent){
		Iterator<Pair> itr = this.cutElements.iterator();
		while(itr.hasNext()){
			Pair pair = itr.next();
			if(pair.getGeneralizaiton().equals(parent)){
				pair.setAnonymous(true);
				return true;
			}
		}
		return false;
	}
	*/
	
	/*
	public boolean isKAnonymized(){
		boolean flag = true;
		Iterator<Pair> itr = this.cutElements.iterator();
		while(itr.hasNext()){
			Pair pair = itr.next();
			if(!pair.isKAnonymized()){
				flag = false;
				break;
			}
		}
		return flag;
	}
	*/	
	/*
	public boolean isKAnonymized(int K){
		boolean flag = true;
		Iterator<Pair> itr = this.cutElements.iterator();
		while(itr.hasNext()){
			Pair pair = itr.next();
			if(pair.getAnonymity() < K){
				flag = false;
				break;
			}
		}
		return flag;
	}
	*/
	
	public int getNumOfElements(){
		return this.cutElements.size();
	}
	
	static public CutWritable merge(CutWritable cutX, CutWritable cutY, int index){
		Vector<Pair> pairsX = (Vector<Pair>)cutX.getElements().clone();
		Vector<Pair> pairsY = (Vector<Pair>)cutY.getElements().clone();
		Iterator<Pair> itrX = pairsX.iterator();
		while(itrX.hasNext()){
			Pair pairX = itrX.next();
			int levelX = Common.taxonomyForest.vectorOfTrees.elementAt(index).getLevel(pairX.getGeneralizaiton());
			Iterator<Pair> itrY = pairsY.iterator();
			while(itrY.hasNext()){
				Pair pairY = itrY.next();
				int levelY = Common.taxonomyForest.vectorOfTrees.elementAt(index).getLevel(pairY.getGeneralizaiton());
				if(levelX < levelY){
					if(Common.taxonomyForest.vectorOfTrees.elementAt(index).isDescendant(pairX.getGeneralizaiton(), pairY.getGeneralizaiton())){
						itrX.remove();
						break;
					}
				}
				else if(levelX > levelY){
					if(Common.taxonomyForest.vectorOfTrees.elementAt(index).isDescendant(pairY.getGeneralizaiton(), pairX.getGeneralizaiton())){
						itrY.remove();
					}
				}
				else{
					if(pairY.getGeneralizaiton().equals(pairX.getGeneralizaiton())){
						itrY.remove();
						break;
					}
				}
			}
		}
		
		CutWritable nCut = new CutWritable();
		for(Pair pair: pairsX){
			Pair nPair = new Pair(pair.getGeneralizaiton());
			nCut.getElements().add(nPair);
		}
		for(Pair pair: pairsY){
			Pair nPair = new Pair(pair.getGeneralizaiton());
			nCut.getElements().add(nPair);
		}
		
		return nCut;
	} 
	
	public void checkAndSetValidity(int index){
		for(Pair pair: cutElements){
			int level = Common.taxonomyForest.vectorOfTrees.elementAt(index).getLevel(pair.getGeneralizaiton());
			if(level == 0){
				pair.setValidity(false);
			}
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
	if(cutElements.size() > 0){
		System.err.println("Error: the size of initial cut is lareger than 0!");
	}
		int size = in.readInt();
		for(int i = 0; i<size; i++){
			Pair pair = new Pair();
			pair.readFields(in);
			cutElements.add(pair);
		}
	}

	@Override
	/*
	 * Store the content of elements of a cut by serialize.
	 * */
	public void write(DataOutput out) throws IOException {
		
		// First store the number of elements for recovery when de-serialization.
		out.writeInt(cutElements.size());
		for(int i = 0; i<cutElements.size(); i++){
			//out.writeUTF(cutElements.elementAt(i));
			cutElements.elementAt(i).write(out);
		}
	}

}
