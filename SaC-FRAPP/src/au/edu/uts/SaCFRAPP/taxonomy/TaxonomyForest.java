package au.edu.uts.SaCFRAPP.taxonomy;

import au.edu.uts.SaCFRAPP.common.*;
import au.edu.uts.SaCFRAPP.datastructure.*;


import java.io.FileReader;
import java.util.*;

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;


public class TaxonomyForest {

	// This field needs modification when running in eclipse/real hadoop clusters
	String locationOfTaxonomy;
	Vector<TaxonomyTree> vectorOfTrees;
	
	public TaxonomyForest(){
		/*Initialize data from configuration file.*/
		vectorOfTrees = new  Vector<TaxonomyTree>();
		locationOfTaxonomy = "res/AttributeTaxonomyTrees.xml";
		//locationOfTaxonomy = "res/AttributeTaxonomy.xml";
		
		fillTaxonomyForest(vectorOfTrees, locationOfTaxonomy);
	}
	
	public TaxonomyForest(String taxonomyPath){
		
		vectorOfTrees = new  Vector<TaxonomyTree>();
		locationOfTaxonomy = taxonomyPath;
		
		fillTaxonomyForest(vectorOfTrees, locationOfTaxonomy);
	}
	
	private void fillTaxonomyForest(Vector<TaxonomyTree> vecOfTrees, String path){
		try {
	         // Create SAX 2 parser...
	         XMLReader xr = XMLReaderFactory.createXMLReader();
	         
	         // Create the ContentHandler...
	         XMLParse xt = new XMLParse(vecOfTrees);
	        
	         // Set the ContentHandler...
	         xr.setContentHandler(xt);
	         
	         /**
	          * Modify to make it able to be exported in a jar file.
	          * */
	         // Parse the file...
	         //xr.parse( new InputSource(new FileReader( path )) );
	         xr.parse( new InputSource(this.getClass().getResourceAsStream(path)) );
	         
	      }catch ( Exception e )  {
	         e.printStackTrace();
	      }
	}
	
	public void displayTrees(){  // Display the trees in a table form
		System.out.println("\nThe attribute trees: ");
		Iterator<TaxonomyTree> itrTree = vectorOfTrees.iterator();
		while(itrTree.hasNext()){
			TaxonomyTree gTree = itrTree.next();
			Iterator<AttributeValueNode> itrAVNode = gTree.vectorOfTreeNodes.iterator();
			while(itrAVNode.hasNext()){
				AttributeValueNode avNode = itrAVNode.next();
				if(avNode.parent != null){
					System.out.println(avNode.value+"("+avNode.level+")"+ "------>"+avNode.parent.value);
				}
				else{
					System.out.println(avNode.value+"("+avNode.level+")"+ "------>"+"null");
				}
			}
			System.out.println();
		}
	}
	
	public void printForest(){
		System.out.println("\nThe Taxonomy Trees: ");
		Iterator<TaxonomyTree> itrTree = vectorOfTrees.iterator();
		while(itrTree.hasNext()){
			TaxonomyTree gTree = itrTree.next();
			System.out.println(gTree.attribute.name()+": ");
			
			gTree.printTree();
			System.out.println("\n");
		}
	}
	
	public int getNumberOfTrees(){
		return vectorOfTrees.size();
	}
	
	public Vector<String> getChildren(int noOfTree, String parentValue){
		TaxonomyTree tt = vectorOfTrees.elementAt(noOfTree);
		return tt.getChildValues(parentValue);
	}
	
	public String getParent(int noOfTree, String childValue){
		TaxonomyTree tt = vectorOfTrees.elementAt(noOfTree);
		return tt.getParentValue(childValue);
	}
	
	public String getChildByLeaf(int noOfTree, String parentValue, String leafValue){
		TaxonomyTree tt = vectorOfTrees.elementAt(noOfTree);
		return tt.getChildByLeaf(parentValue, leafValue);
	}
	
	public Attribute getAttribute(int index){
		return vectorOfTrees.elementAt(index).getAttribute();
	}
	
	public int getLevel(int index, String domainValue){
		return vectorOfTrees.elementAt(index).getLevel(domainValue);
	}
	
	public String getAnscestor(int index, String dominaValue, int level){
		return vectorOfTrees.elementAt(index).getAnscestor(dominaValue, level);
	}
	
	public HashMap<String, String> getMap(int index, int level){
		return vectorOfTrees.elementAt(index).getMap(level);
	}
	
	public boolean isDescendant(int index, String leafValue, String ancestor){
		return vectorOfTrees.elementAt(index).isDescendant(leafValue, ancestor);
	}
	
	public boolean isLeaf(int index, String domainValue){
		return vectorOfTrees.elementAt(index).isLeaf(domainValue);
	}
	
	public double distance(int index, String domainValueX, String domainValueY){
		return vectorOfTrees.elementAt(index).distance(domainValueX, domainValueY);
	}
	
	/*
	 * Get the nodes on the lowest level among internal nodes. To construct the initial generalization level when adopting bottom-up algorithm.
	 * */
	public Vector<String> getLowestGeneralizations(){
		Vector<String> lowestGens = new Vector<String>();
		Iterator<TaxonomyTree> itrTree = vectorOfTrees.iterator();
		while(itrTree.hasNext()){
			TaxonomyTree gTree = itrTree.next();
			lowestGens.addAll(gTree.getAVNodeValuesByLevel(1));
		}
		return lowestGens;
	}
	
	public Vector<String> getLowestGeneralization(int index){
		return vectorOfTrees.elementAt(index).getAVNodeValuesByLevel(1);
	}
	
	public String getTopmostGeneralization(int index){
		return vectorOfTrees.elementAt(index).getTopDomain();
	}
	
	public Vector<Pair> getGeneralization(int index, String leafValue, CutWritable cut){
		return vectorOfTrees.elementAt(index).getGeneralization(leafValue, cut);
	}	
	
	public Vector<String> getLeaves(int index){
		return vectorOfTrees.elementAt(index).getAVNodeValuesByLevel(0);
	}
	
	public int getNumOfLeaves(int index){
		return this.getLeaves(index).size();
	}
	
	public Vector<String> getLeaves(int index, String domainValue){
		return vectorOfTrees.elementAt(index).getLeaves(domainValue);
	}
	
	public int getNumOfLeaves(int index, String domainValue){
		return this.getLeaves(index, domainValue).size();
	}
	
	public int getNumOfDescents(int index, String domainValue){
		return vectorOfTrees.elementAt(index).getNumOfDescents(domainValue);
	}
	
	public int getTotalNumOfDomainValues(int index){
		return vectorOfTrees.elementAt(index).getTotalNumOfDomainValues();
	}
	
	public int getHeight(int index){
		return vectorOfTrees.elementAt(index).getHeight();
	}
	
	public String getLowestCommonAncestor(int index, String domainValueX, String domainValueY){
		return vectorOfTrees.elementAt(index).getLowestCommonAncestor(domainValueX, domainValueY);
	}
	
	public double pathLength(int index, String descendant, String ancestor){
		return vectorOfTrees.elementAt(index).pathLength(descendant, ancestor);
	}
	
	public Vector<String> getDomainValuesByLevel(int index, int level){
		return vectorOfTrees.elementAt(index).getAVNodeValuesByLevel(level);
	}
	
	public boolean isTopMost(int index, String domainValue){
		return vectorOfTrees.elementAt(index).isTopMost(domainValue);
	}
	
	// Here LELEL is counted from the top of a tree
	public int getNumOfDomainValuesByLevel(int index, int level){
		int height = vectorOfTrees.elementAt(index).getHeight();
		Vector<String> domainValues;
		if(level > height){
			domainValues = this.getDomainValuesByLevel(index, 0);
		}
		else{
			domainValues = this.getDomainValuesByLevel(index, height-level);
		}
		return domainValues.size();
	}
	
	public static void main( String[] argv ){
		//TaxonomyForest tf = new TaxonomyForest();
		//tf.printForest();
		TaxonomyForest tf = new TaxonomyForest("res/AttributeTaxonomy.xml");
		tf.printForest();
		
		//System.out.println("Distance is : "+tf.distance(4, "Other-Rel-18+", "Spouse-of-householder"));
		//System.out.println("The number of all leaf vales is: "+tf.getNumOfLeaves(4));
		//System.out.println("The number of leaf vales of Other-Rel-18+ is: "+tf.getNumOfLeaves(4, "Other-Rel-18+"));
	}
}
