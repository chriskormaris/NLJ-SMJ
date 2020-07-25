package project;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.List;

public class NestedLoopJoin {
	
	
	/** NLJ that is memory friendly. 
	 * We assume that T(R) = B(R) and T(S) = B(S),
	 * which means that each block contains exactly one record.
	 * **/
	public void blockedNestedLoopJoin(String csvfile1, String csvfile2,
			int a1, int a2, int m, String outputFile) {
		
		Utilities.createNewFile(outputFile);
		
		// We want relation1 to be the smaller one to create less blocks.
		// relation1 will be the outer relation
		int T1 = Utilities.getNumberOfRecords(csvfile1); // number of records of relation1
		int T2 = Utilities.getNumberOfRecords(csvfile2); // number of records of relation2
		if (T2 < T1) {
			String tempString = csvfile1;
			csvfile1 = csvfile2;
			csvfile2 = tempString;
			
			int tempInteger = a1;
			a1 = a2;
			a2 = tempInteger;
			
			tempInteger = T1;
			T1 = T2;
			T2 = tempInteger;
		}
		
		// get the name of the 1st relation from the csv file name
		String relation1Name = Utilities.getCSVName(csvfile1);
		
		// get the name of the 2nd relation from the csv file name
		String relation2Name = Utilities.getCSVName(csvfile2);
		
		if (T2 >= T1) {
			System.out.println("Outer relation is: " + relation1Name);
			System.out.println("Inner relation is: " + relation2Name);
		} else {
			System.out.println("Outer relation is: " + relation2Name);
			System.out.println("Inner relation is: " + relation1Name);
		}
		
		BufferedReader br1 = Utilities.createBufferedReaderAndSkipFirstLine(csvfile1);
		BufferedReader br2 = Utilities.createBufferedReaderAndSkipFirstLine(csvfile2);
		BufferedWriter bw = Utilities.createBufferedWriter(outputFile);
		boolean firstTuple = true; // used to print the header

		// block of relation1 (outer relation with less records than relation2)
		List<Tuple> R = new ArrayList<Tuple>();
		
		Tuple r = null;
		Tuple s = null;
		int PR=0; // pointer for relation1
		while ((r = Utilities.getNextTuple(br1, relation1Name)) != null) {
			
			R.add(r);
			PR++;

			if ( (PR % (m-1) == 0) || (PR == Utilities.getNumberOfRecords(csvfile1)) ) {

				while ( (s = Utilities.getNextTuple(br2, relation2Name)) != null) {
					for (int j=0; j<R.size(); j++) {
						
						if (R.get(j).getAttribute(a1).getValue() == s.getAttribute(a2).getValue()) {
							Tuple outputTuple = Utilities.joinTuples(R.get(j), s, a1, a2);
							
							// if this is the first tuple to be written,
							// we should write the attributes header beforehand 
							if (firstTuple) {
								Utilities.writeHeaderToFile(bw, outputTuple);
								firstTuple = false;
							}
							
							Utilities.writeTupleToFile(bw, outputTuple);
						}
						
					}

				}
				R.clear(); // empty the block, every (m-1) times
				Utilities.closeBufferedReader(br2);
				br2 = Utilities.createBufferedReaderAndSkipFirstLine(csvfile2);
			}
		}
		Utilities.closeBufferedReader(br1);
		Utilities.closeBufferedReader(br2);
		Utilities.closeBufferedWriter(bw);
		
	}
	
	
	/** Super naive NLJ. Very slow!  **/
	public void nestedLoopJoin(String csvfile1, String csvfile2,
			int a1, int a2, String outputFile) {
		
		// get the name of the 1st relation from the csv file name
		String relation1Name = Utilities.getCSVName(csvfile1);
		
		// get the name of the 2nd relation from the csv file name
		String relation2Name = Utilities.getCSVName(csvfile2);
		
		BufferedReader br1 = Utilities.createBufferedReaderAndSkipFirstLine(csvfile1);
		BufferedReader br2 = Utilities.createBufferedReaderAndSkipFirstLine(csvfile2);
		BufferedWriter bw = Utilities.createBufferedWriter(outputFile);
		boolean firstTuple = true; // used to print the header

		Tuple r = null;
		Tuple s = null;
		while ((r = Utilities.getNextTuple(br1, relation1Name)) != null) {
			while ( (s = Utilities.getNextTuple(br2, relation2Name)) != null) {
				if (r.getAttribute(a1).getValue() == s.getAttribute(a2).getValue()) {
					Tuple outputTuple = Utilities.joinTuples(r, s, a1, a2);
					
					// if this is the first tuple to be written,
					// we should write the attributes header beforehand 
					if (firstTuple) {
						Utilities.writeHeaderToFile(bw, outputTuple);
						firstTuple = false;
					}
					
					Utilities.writeTupleToFile(bw, outputTuple);
				}
			}
			Utilities.closeBufferedReader(br2);
			br2 = Utilities.createBufferedReaderAndSkipFirstLine(csvfile2);
		}
		Utilities.closeBufferedReader(br1);
		Utilities.closeBufferedWriter(bw);
	}
	
	
}
