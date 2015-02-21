package au.edu.uts.SaCFRAPP.common;


public class AttributeValueNode {

	public String value;
	public int level;
	public AttributeValueNode parent;
	public AttributeValueNode siblings;
	public AttributeValueNode children;
	
	public AttributeValueNode(String v){
		value = v;
		level = 0;
		parent = null;
		siblings = null;
		children = null;
	}
}
