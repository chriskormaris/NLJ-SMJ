package project;

public class Attribute {
	
	private int value;
	private String name;
	
	public Attribute() {
		this.value = 0;
		this.name = "R0";
	}
	
	public Attribute(int value, String name) {
		this.value = value;
		this.name = name;
	}
	
	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public String toString() {
		return name + ": " + value;
	}

}
