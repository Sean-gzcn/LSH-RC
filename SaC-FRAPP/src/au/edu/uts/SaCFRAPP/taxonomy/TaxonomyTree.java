package au.edu.uts.SaCFRAPP.taxonomy;
import au.edu.uts.SaCFRAPP.common.*;
import au.edu.uts.SaCFRAPP.datastructure.CutWritable;


import java.util.*;

public class TaxonomyTree {

	public Attribute attribute;
	public AttributeValueNode generalizationRoot;
	public int height;
	public Vector<AttributeValueNode> vectorOfTreeNodes;
	
	/*
	 * Construct function.
	 * */
	public TaxonomyTree(){
		vectorOfTreeNodes = new Vector<AttributeValueNode>();
	}
	
	/*
	 * Get the node when given the node value.
	 * */
	public AttributeValueNode getAVNode(String value){
		AttributeValueNode q = getAVNode(value, generalizationRoot);
		if(q == null){
			System.err.println("Error: domain value '"+value+"' not found!!!");
		}
		return q;
	}
	
	private AttributeValueNode getAVNode(String value, AttributeValueNode p){
		if(p.value.equals(value)){
			return p;
		}
		else{
			AttributeValueNode q = null;
			AttributeValueNode c = p.children;
			while(c != null){
				if((q = getAVNode(value, c)) != null){
					return q;
				}
				c  = c.siblings;
			}
			return null;
		}
	}
	
	/*
	 * Get the child values when given a parent value.
	 * */
	public Vector<String> getChildValues(String parentValue){
		AttributeValueNode p = this.getAVNode(parentValue);
		Vector<String> vecOfChildValues= new Vector<String>();
		AttributeValueNode c = p.children;
		while(c != null){
			vecOfChildValues.add(c.value);
			c  = c.siblings;
		}
		return vecOfChildValues;
	}
	
	/*
	 * Get the parent value when given a child value.
	 * */
	public String getParentValue(String child){
		AttributeValueNode c = this.getAVNode(child);
		AttributeValueNode p = this.getParentAVNode(c);
		if(p == null){
			return null;
		}
		return p.value;
	}
	
	/*
	 * Get a child of a parent, where the child branch contains a given leaf.
	 * */
	public String getChildByLeaf(String parentValue, String leafValue){
		AttributeValueNode p = this.getAVNode(parentValue);
		AttributeValueNode c = p.children;
		while(c!=null){
			if((this.getAVNode(leafValue, c))!= null){
				break;
			}
			c = c.siblings;
		}
		if(c == null){
			System.err.println("Error: parent and leaf nodes are the same one! Or error happen!");
		System.out.println(parentValue+", "+leafValue);
		}
		return c.value;
	}
	
	private void print(AttributeValueNode node, String leftStr, String junction){
		System.out.println(junction + node.value+"("+node.level+")");
		AttributeValueNode p = node.children;
		while(p!= null && p.siblings != null){
			System.out.print(leftStr);
			print(p, leftStr+"|", "\u22A2");
			p = p.siblings;
		}
		if(p != null){
			System.out.print(leftStr);
			print(p, leftStr+" ", "\u221F");
		}
	}
	
	public void printTree(){
		print(this.generalizationRoot, "", "");
	}
	
	public Attribute getAttribute(){
		return attribute;
	}
	
	public int getLevel(String domainValue){
		AttributeValueNode p = getAVNode(domainValue);
		return p.level;
	}
	
	/*
	 * Get the ancestor of a value on given level.
	 * */
	public String getAnscestor(String domainValue, int level){
		AttributeValueNode p = getAVNode(domainValue);
		while(p.level < level){
			//p = p.parent;
			p = this.getParentAVNode(p);
		}
		return p.value;
	}
	
	public AttributeValueNode getParentAVNode(AttributeValueNode p){
		if(p.parent == null){
			return null;
		}
		while(p.parent.siblings == p){
			p = p.parent;
		}
		return p.parent;
	}
	
	/*
	 * Get all the values on a given level.
	 * */
	public Vector<String> getAVNodeValuesByLevel(int level){
		Vector<String> nodeValues = new Vector<String>();
		Iterator<AttributeValueNode> itrNodes = vectorOfTreeNodes.iterator();
		while(itrNodes.hasNext()){
			AttributeValueNode p = itrNodes.next();
			if(p.level == level){
				nodeValues.add(p.value);
			}
		}
		return nodeValues;
	}
	
	public Vector<String> getLeaves(String domainValue){
		Vector<String> partLeaves = new Vector<String>();
		Vector<String> allLeaves = getAVNodeValuesByLevel(0);
		Iterator<String> itr = allLeaves.iterator();
		while(itr.hasNext()){
			String leaf = itr.next();
			if(isDescendant(leaf, domainValue)){
				partLeaves.add(leaf);
			}
		}
		
		return partLeaves;
	}
	
	public boolean isDescendant(String leafValue, String ancestor){
		AttributeValueNode anc = this.getAVNode(ancestor);
		return isDescendant(leafValue, anc);
	}
	
	private boolean isDescendant(String leafValue, AttributeValueNode ancestor){
		if(ancestor.value.equals(leafValue)){
			return true;
		}
		else{
			AttributeValueNode p = ancestor.children;
			while(p != null){
				if(isDescendant(leafValue, p)){
					return true;
				}
				p = p.siblings;
			}
			return false;
		}
	}
	
	public boolean isLeaf(String domainValue){
		AttributeValueNode p = this.getAVNode(domainValue);
		if(p.level == 0){
			return true;
		}
		else{
			return false;
		}
	}
	
	public HashMap<String, String> getMap(int level){
		
		if(level < 0){
			//System.err.println("Error: The required level must be larger than 0");
			return null;
		}
		HashMap<String, String> map = new HashMap<String, String>();
		Vector<String> leafValues = getAVNodeValuesByLevel(0);
		
		Iterator<String> itr = leafValues.iterator();
		while(itr.hasNext()){
			String str = itr.next();
			String ancestor = getAnscestor(str, level);
			map.put(str, ancestor);
		}
		return map;
	}
	
	private AttributeValueNode lowestCommonAncestor(AttributeValueNode qx, AttributeValueNode qy){

		AttributeValueNode p;
		for(p = qx; p != null; p = this.getParentAVNode(p)){
			if(p.value.equals(qy.value) || this.isDescendant(qy, p)){
				return p;
			}
			
		}
		return null;
	} 
	
	public String getLowestCommonAncestor(String domainValueX, String domainValueY){
		AttributeValueNode qx = this.getAVNode(domainValueX);
		AttributeValueNode qy = this.getAVNode(domainValueY);
		
		AttributeValueNode p = this.lowestCommonAncestor(qx, qy);
		
		if(p == null){
			System.out.println("Error: No common ancestor. ("+domainValueX+", "+domainValueY+")");
		}
		
		return p.value;
	}
	
	private boolean isDescendant(AttributeValueNode desc, AttributeValueNode ansc){
		boolean flag = false;
		for(AttributeValueNode p = desc; p != null; p = this.getParentAVNode(p)){
			if(p.value.equals(ansc.value)){
				flag = true;
				break;
			}
		}
		
		return flag;
	}
	
	public double distance(String domainValueX, String domainValueY){
		AttributeValueNode qx = this.getAVNode(domainValueX);
		AttributeValueNode qy = this.getAVNode(domainValueY);
		
		AttributeValueNode p = this.lowestCommonAncestor(qx, qy);
		
		if(p == null){
			System.out.println("Error: No common ancestor. ("+domainValueX+", "+domainValueY+")");
			return -1.0;
		}
		
		return this.pathLength(qx, p)+this.pathLength(qy, p);
	}
	
	private double pathLength(AttributeValueNode descendant, AttributeValueNode ancestor){
		
		int length = 0;
		AttributeValueNode p = descendant;
		while(!p.value.equals(ancestor.value)){
			length++;
			p = this.getParentAVNode(p);
		}
		return length*1.0;
	}
	
	public double pathLength(String descendant, String ancestor){
		AttributeValueNode desc = this.getAVNode(descendant);
		AttributeValueNode ance = this.getAVNode(ancestor);
		
		return this.pathLength(desc, ance);
	}
	
	/*
	 * Get a generalization consist of a parent value and a child value.The first element is parent, the second is chidl.
	 * */
	public Vector<Pair> getGeneralization(String leafValue, CutWritable cut){
		
		Vector<Pair> rlt = new Vector<Pair>();
		Vector<Pair> cutElements = cut.getElements();
		Iterator<Pair> itr = cutElements.iterator();
		while(itr.hasNext()){
			Pair ancestor = itr.next();
			if(this.isDescendant(leafValue, ancestor.getGeneralizaiton())){
				rlt.add(ancestor);
				rlt.add(new Pair(this.getChildByLeaf(ancestor.getGeneralizaiton(), leafValue)));
				return rlt;
			}
		}
		System.err.println("Error: in function getGeneraliztion(leafvalue, cut)");
		return null;
	}
	
	public String getTopDomain(){
		return generalizationRoot.value;
	}
	
	private int getNumOfDescents(AttributeValueNode domainValue){
		AttributeValueNode p = domainValue.children;
		int count = 1;
		while(p != null){
			count += getNumOfDescents(p);
			p = p.siblings;
		}
		return count;
	}
	
	public int getNumOfDescents(String domainValue){
		return this.getNumOfDescents(this.getAVNode(domainValue));
	}
	
	public int getTotalNumOfDomainValues(){
		return vectorOfTreeNodes.size();
	}
	
	public int getHeight(){
		return height;
	}
	
	public boolean isTopMost(String domainValue){
		if(domainValue.equals(this.getTopDomain())){
			return true;
		}
		else{
			return false;
		}
	}
	
}
