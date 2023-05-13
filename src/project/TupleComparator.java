package project;

import java.util.Comparator;

// Used to sort the sublists of tuple objects, based on a given attribute, during SMJ.
public class TupleComparator implements Comparator<Tuple> {

	int sortAttribute;

	TupleComparator(int sortAttribute) {
		super();
		this.sortAttribute = sortAttribute;
	}

	@Override
	public int compare(Tuple t1, Tuple t2) {
		int result;
		result = Integer.compare(t1.getAttributeValue(sortAttribute),
				t2.getAttributeValue(sortAttribute));
		return result;
	}

}
