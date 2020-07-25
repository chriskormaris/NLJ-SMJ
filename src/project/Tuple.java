package project;

import java.util.ArrayList;
import java.util.List;

public class Tuple implements Comparable<Tuple> {
	
	private int number_of_attributes;
	private String relationName;
	
	List<Attribute> attributes;
	
	public Tuple() {
		setNumberOfAttributes(4);
		
		attributes = new ArrayList<Attribute>();
	}
	
	public Tuple(int number_of_attributes) {
			this.number_of_attributes = number_of_attributes;
			
			attributes = new ArrayList<Attribute>();
	}
	
	public Tuple(int number_of_attributes, String relationName) {
		this.number_of_attributes = number_of_attributes;
		this.relationName = relationName;
		attributes = new ArrayList<Attribute>();
	}
	

	public int getNumberOfAttributes() {
		return number_of_attributes;
	}

	public void setNumberOfAttributes(int number_of_attributes) {
		this.number_of_attributes = number_of_attributes;
	}
	
	public String getRelationName() {
		return relationName;
	}

	public void setRelationName(String relationName) {
		this.relationName = relationName;
	}
	
	public int getAttributeValue(int index) {
		return attributes.get(index).getValue();
	}
	
	public String getAttributeName(int index) {
		return attributes.get(index).getName();
	}
	
	public void setAttributeValue(int index, int value) {
		attributes.get(index).setValue(value);
	}
	
	public void setAttributeName(int index, String value) {
		attributes.get(index).setName(value);
	}
	
	public Attribute getAttribute(int index) {
		return attributes.get(index);
	}
	
	@Override
	public String toString() {
		String tuple = "";
		for (Attribute attribute: attributes) {
			tuple = tuple + attribute.getValue() + "\t";
		}
		return tuple;
//		return attributes.toString();
	}

	public String getHeader() {
		String header = "";
		for (Attribute attribute: attributes) {
			if (attributes.indexOf(attribute) != attributes.size()-1) {
				header = header + attribute.getName() + ",";
			} else {
				header = header + attribute.getName();
			}
		}
		return header;
	}
	
	// get comma separated representation for attributes
	public String getCSVRepresentation() {
		String tuple = "";
		for (Attribute attribute: attributes) {
			if (attributes.indexOf(attribute) != attributes.size()-1) {
				tuple = tuple + attribute.getValue() + ",";
			}
			else if (attributes.indexOf(attribute) == attributes.size()-1) {
				tuple = tuple + attribute.getValue();
			}
		}
		return tuple;
	}

	@Override
	public int compareTo(Tuple o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
