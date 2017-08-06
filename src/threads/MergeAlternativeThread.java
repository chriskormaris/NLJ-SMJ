package threads;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import project.Tuple;
import project.Utilities;

// Slower merge algorithm.
// Reads one tuple at a time from each sorted sublist of the relation
// and writes the min in the sortedRcsv or sortedScsv file.
public class MergeAlternativeThread extends Thread {

	String csvfile;
	String relationName;
	int a;
	int m;
	String tempDir;
	String sortedRelationCSV;
	
	public MergeAlternativeThread(String csvfile, String relationName,
			int a, int m, String tempDir, String sortedRelationCSV) {
		
		this.csvfile = csvfile;
		this.relationName = relationName;
		this.a = a;
		this.m = m;
		this.tempDir = tempDir;
		this.sortedRelationCSV = sortedRelationCSV;
		
	}
	
	@Override
	public void run() {
		System.out.println("Merge thread for relation " + relationName + " has started!");

		int total_records = Utilities.getNumberOfRecords(csvfile);
		
		int number_of_sublists = (int) Math.ceil((double) total_records / m);
		
		System.out.println("relation " + relationName + " number of sublists: " + number_of_sublists);
		System.out.println();
		
		/*** Merge the sorted sublist files into one sorted file. ***/
		
		BufferedWriter bw = Utilities.createBufferedWriter(tempDir + "/" + sortedRelationCSV);
		List<BufferedReader> brs = new ArrayList<BufferedReader>();
		for (int j=0; j<number_of_sublists; j++) {
			String relation1Sublist = relationName + "Sublist" + j;
			BufferedReader br = Utilities.createBufferedReader(tempDir + "/" + relation1Sublist);
			brs.add(br);
		}
		boolean[] firstTimes = new boolean[number_of_sublists];
		Arrays.fill(firstTimes, Boolean.TRUE);
		
		Tuple[] lastTuples = new Tuple[number_of_sublists];
		int sublistIndexWhereTheLastTupleWasFetchedFrom = -1;
		
		for (int i=0; i<total_records; i++) {
			int min = Integer.MAX_VALUE;
			Tuple minTuple = null;
			int minPos = -1;
			int sublistIndexToBeDeleted = -1;
			for (int j=0; j<number_of_sublists; j++) {
				String relationSublist = relationName + "Sublist" + j;
				if (Utilities.fileExists(tempDir + "/" + relationSublist)) {
					if (firstTimes[j] || j == sublistIndexWhereTheLastTupleWasFetchedFrom) {
						BufferedReader br = brs.get(j);
						Tuple tuple = Utilities.getNextTuple(br, relationName);
						lastTuples[j] = tuple;
					}

					if (firstTimes[j]) firstTimes[j] = false;

					if (lastTuples[j] != null && lastTuples[j].getAttributeValue(a) < min) {
						min = lastTuples[j].getAttributeValue(a);
						minTuple = lastTuples[j];
						minPos = j;
					}
					if (lastTuples[j] == null) {
						sublistIndexToBeDeleted = j;
					}
					
				}
				
			}

			if (sublistIndexToBeDeleted != -1) {
				System.out.println("Thread " + this.getId() + ": " +
						"Deleting file " + relationName +
						"Sublist" + sublistIndexToBeDeleted + "...");
				BufferedReader br = brs.get(sublistIndexToBeDeleted);
				Utilities.closeBufferedReader(br);
				Utilities.deleteFile(tempDir + "/" + relationName + "Sublist" + sublistIndexToBeDeleted);
			}
			if (minTuple != null) {
				sublistIndexWhereTheLastTupleWasFetchedFrom = minPos;
				Utilities.writeTupleToFile(bw, minTuple);
			}
		}

		Utilities.closeBufferedWriter(bw);
		
	}

}
