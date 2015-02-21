package au.edu.uts.SaCFRAPP.taxonomy;
import au.edu.uts.SaCFRAPP.common.Attribute;
import au.edu.uts.SaCFRAPP.common.AttributeValueNode;


import org.xml.sax.*;
import org.xml.sax.helpers.*;
import java.io.*;
import java.util.*;
public class XMLParse extends DefaultHandler {
 
   private CharArrayWriter contents;  // To get the content of tag
   Vector<TaxonomyTree> vectorOfTrees;  //Fill this vector in this class
   TaxonomyTree curTree;  // Current tree being dealt with
   int counter = 0;  // A count used to distinguish the root from other nodes
   String curString; // Current string in contents
   AttributeValueNode curNode = null;  // Current node being dealt with
   AttributeValueNode parentNode = null; 
   AttributeValueNode lastSiblingNode = null; 
   int curDepth = 0;  // The depth of the tree
   Stack<AttributeValueNode> stack;  // To track the parent of a node
   
   public XMLParse(Vector<TaxonomyTree> vec){
	   contents = new CharArrayWriter();
	   vectorOfTrees = vec;
	   stack = new Stack<AttributeValueNode>();
   }

   public void startElement( String namespaceURI,
               String localName,
              String qName,
              Attributes attr ) throws SAXException {
	   
	  contents.reset();
	  
	  if(localName.equals("TaxonomyTree")){ // A new generalization tree
		  TaxonomyTree gTree = new TaxonomyTree();
		  curTree = gTree;
		  gTree.height = Integer.valueOf(attr.getValue("Height"));   //Set the heigh
		  gTree.attribute = Attribute.valueOf(attr.getValue("Attribute"));  //Set the Attribute
		  vectorOfTrees.add(gTree);
		 
		  curDepth = 0;
		  curNode = null; // Clear current node
		  parentNode = null;
		  lastSiblingNode = null;
		  stack.clear();  // Clear the stack
	  }
	  
	  if(localName.equals("AttributeTopNode")){  //The root of the tree
		  curNode = new AttributeValueNode("");
		  curNode.level = curTree.height-curDepth; // Set the level of this node
		  curTree.vectorOfTreeNodes.add(curNode);  // Add this node to the tree
		  curTree.generalizationRoot = curNode;
		  
		  curNode.parent = null;  //Set the parent of root as null
		  curNode.siblings = null; //Set the siblings of root as null
		  parentNode = curNode;
	  }
	  if(localName.equals("AttributeNode")){  // Find a tree node
		  curNode = new AttributeValueNode("");
		  curNode.level = curTree.height-curDepth; // Set the level of this node
		  curTree.vectorOfTreeNodes.add(curNode);  // Add this node to the tree

		  if(lastSiblingNode != null){  //Having a sibling before this node
			  curNode.parent = lastSiblingNode;
			  AttributeValueNode p = lastSiblingNode.siblings;
			  
			  /*Link curnode to the siblings list in lastSiblingNode*/
			  if(p == null){  //current node is the first sibling of last sibliing node
				  lastSiblingNode.siblings = curNode;
			  }
			  else{
				  AttributeValueNode q = p.siblings;
				  while(q != null){
					  p = q;
					  q = p.siblings;
				  }
				  p.siblings = curNode;
			  }
			  
			  lastSiblingNode = curNode;
		  }
		  else{  //Be the first node of parent
			  curNode.parent = parentNode; 
			  parentNode.children = curNode;
			  lastSiblingNode = curNode;
		  }
	  }
	  
	  if(localName.equals("ChildAttributeNodes")){
		  stack.push(parentNode);
		  stack.push(lastSiblingNode);
		  
		  parentNode = curNode;
		  lastSiblingNode = null;

		  curDepth++; // Tree depth increase
	  }
   }
   public void endElement( String namespaceURI,
               String localName,
              String qName ) throws SAXException {
	   if (localName.equals( "AttributeValue" ) ) {
	         curString = contents.toString();
	         curNode.value = curString; // Set the Value of current node created in startElement callback function
	         //System.out.println(curString);
	   }
	   
	   if(localName.equals("ChildAttributeNodes")){
		   lastSiblingNode = stack.pop();
		   parentNode = stack.pop();
		   curDepth--; // Tree depth decrease
	   }
   }
   public void characters( char[] ch, int start, int length )
                  throws SAXException {
	   contents.write( ch, start, length );
   }
   
   
   public static void main( String[] argv ){
   }
}
