package au.edu.uts.SaCFRAPP.common;

public class AttributeNode<E> {

	int position;
	String topValue;
	Orderability orderability;
	E upperLimit;
	E lowerLimit;
	
	public AttributeNode (int position){
		this.position = position;
	}
	
	public void setPostion(int position){
		this.position = position;
	}
	
	public int getPosition(){
		return this.position;
	} 
	
	public void setTopValue(String topValue){
		this.topValue = topValue;
	}
	
	public String getTopValue(){
		return this.topValue;
	}
	
	public void setOrderability(Orderability orderability){
		this.orderability = orderability;
	}
	
	public Orderability getOrderability(){
		return this.orderability;
	}
	
	public void setUpperLimit(E upperLimit){
		if(this.orderability == Orderability.Partial){
			System.out.println("Error:You are trying to set upper limit for a partial odered attribute!!");
			return;
		}
		this.upperLimit = upperLimit;
	}
	
	public E getUpperLimit(){
		if(this.orderability == Orderability.Partial){
			System.out.println("Error: No upper limit for a partial odered attribute!!");
			return null;
		}
		return this.upperLimit;
	}
	
	public void setLowerLimit(E lowerLimit){
		if(this.orderability == Orderability.Partial){
			System.out.println("Error:You are trying to set lower limit for a partial odered attribute!!");
			return;
		}
		this.lowerLimit = lowerLimit;
	}
	
	public E getLowerLimit(){
		if(this.orderability == Orderability.Partial){
			System.out.println("Error: No lower limit for a partial odered attribute!!");
			return null;
		}
		return this.lowerLimit;
	}
}
