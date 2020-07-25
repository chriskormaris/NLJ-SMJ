package project;

import java.io.BufferedReader;
import java.io.BufferedWriter;

import multiple_merge_threads.RelationMergeThread;


public class SortMergeJoin {
	
	
	/** Non-efficient SMJ.  **/	
	public void sortMergeJoin(String csvfile1, String csvfile2,
			int a1, int a2, int m, String tempDir, String outputFile) {
		
		Utilities.createTempDir(tempDir);
		
		String relation1Name = Utilities.getCSVName(csvfile1);
		String relation2Name = Utilities.getCSVName(csvfile2);
		
		/*** run the 2-phase sort on R and S ***/
		
		/*** Phase 1: split each relation to sublists and sort them ***/
		
		if (!relation1Name.equals(relation2Name)) {
			Utilities.splitCSVToSortedSublists(csvfile1, a1, m, tempDir);
			Utilities.splitCSVToSortedSublists(csvfile2, a2, m, tempDir);
		}
		// Handle here the situation where we want to join a relation with itself.
		// example: -f1 testdata/B.csv -a1 1 -f2 testdata/B.csv -a2 2 -j SMJ -m 200 -t tmp -o results8.csv
		else if (relation1Name.equals(relation2Name)) {
			Utilities.splitCSVToDuplicateSortedSublists(csvfile1, a1, a2, m, tempDir);
			relation1Name = relation1Name + "1";
			relation2Name = relation2Name + "2";
		}
		
		System.out.println();
		
		/*** Phase 2: merge ***/
		
		String sortedRcsv = "sorted" + relation1Name;
		String sortedScsv = "sorted" + relation2Name;
		
		/* Solution 1 - External merge sorting, without threads. */
		// This is the merging phase of the external sorting algorithm.
		/*
		merge(csvfile1, relation1Name, a1, m, tempDir, sortedRcsv);
		System.out.println();
		merge(csvfile2, relation2Name, a2, m, tempDir, sortedScsv);
		*/
		
		/* Solution 1 - External merge sorting, with 2 threads. */
		/*
		MergeThread merge1 = new MergeThread(csvfile1, relation1Name, a1, m, tempDir, sortedRcsv);
		MergeThread merge2 = new MergeThread(csvfile2, relation2Name, a2, m, tempDir, sortedScsv);
		merge1.start();
		merge2.start();
		try {
			merge1.join();
			merge2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		*/
		
		/* Solution 1 - External merge sorting, with multiple threads (THE FASTEST!). */
		RelationMergeThread merge1 = new RelationMergeThread(csvfile1, relation1Name, a1, m, tempDir, sortedRcsv);
		RelationMergeThread merge2 = new RelationMergeThread(csvfile2, relation2Name, a2, m, tempDir, sortedScsv);
		merge1.start();
		merge2.start();
		try {
			merge1.join();
			merge2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		/* Solution 2, without threads (THE SLOWEST!). */
		// Slower merge algorithm.
		// Reads one tuple at a time from each sorted sublist of the relation
		// and writes the min in the sortedRcsv or sortedScsv file.
		/*
		mergeAlternative(csvfile1, relation1Name, a1, m, tempDir, sortedRcsv);
		System.out.println();
		mergeAlternative(csvfile2, relation2Name, a2, m, tempDir, sortedScsv);
		*/
		
		/* Solution 2, with 2 threads. */
		/*
		MergeAlternativeThread mergeAlternative1 = new MergeAlternativeThread(csvfile1, relation1Name, a1, m, tempDir, sortedRcsv);
		MergeAlternativeThread mergeAlternative2 = new MergeAlternativeThread(csvfile2, relation2Name, a2, m, tempDir, sortedScsv);
		mergeAlternative1.start();
		mergeAlternative2.start();
		try {
			mergeAlternative1.join();
			mergeAlternative2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		*/
		
		System.out.println();
		
		/*** Run the join algorithm on the sorted relations R and S ***/

		System.out.print("Joining relations...");
		Utilities.createNewFile(outputFile);
		join(tempDir + "/" + sortedRcsv, tempDir + "/" + sortedScsv, a1, a2, m, outputFile);
		System.out.println("[OK]");
		
		Utilities.deleteTempDir(tempDir);
		
	}
	
	
	// This is the merging phase of the external sorting algorithm.
	// Splits the sublists to 2 teams.
	// Merges the sublists of team 1 with the corresponding sublists of team 2.
	// In each iterations the number of total sublists is divided by 2.
	// Visit the following url for further explanation:
	// http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX1.htm
	/*
	public void merge(String csvfile, String relationName,
			int a, int m, String tempDir, String sortedRelationCSV) {
		
		int total_records = Utilities.getNumberOfRecords(csvfile);
		
		int number_of_sublists = (int) Math.ceil((double) total_records / m);
		
		System.out.println("relation " + relationName + " number of sublists: " + number_of_sublists);
		
		
		// Merge the sorted sublist files of the relation into one sorted file.

		// Starting from sublists of size m ,
		// each time we double the length until we reach size T1.
		int sublistSize = m;
		while (sublistSize <= total_records) { // merge iterations
						
			int team1SublistCounter = 0;
			int team2SublistCounter = (int) Math.ceil(((double) number_of_sublists / 2));
			
			int number_of_team1_sublists = team2SublistCounter;
			int number_of_team2_sublists = number_of_sublists - number_of_team1_sublists;
			
			System.out.println();
			
			// Sometimes team 1 may have one more sublist than team 2.
			// In that case we do nothing for the last team1Sublist
			// in the current iteration.
			for (int i=0; i<number_of_team2_sublists; i++) { // iterate floor(T1sublists/2) times
				Tuple minTuple = null;
				
				String team1Sublist = relationName + "Sublist" + team1SublistCounter;
				String team2Sublist = relationName + "Sublist" + team2SublistCounter;
				
				System.out.print("Merging sublist " + team1Sublist + " with " + team2Sublist + "...");
				
				Tuple team1Tuple = null;
				Tuple team2Tuple = null;
				
				boolean lastTupleFetchedFromTeam1Sublist = false;
				boolean firstTime = true;
				BufferedReader br1 = Utilities.createBufferedReader(tempDir + "/" + team1Sublist);
				BufferedReader br2 = Utilities.createBufferedReader(tempDir + "/" + team2Sublist);
				BufferedWriter bw = Utilities.createBufferedWriter(tempDir + "/" + relationName + "_MERGE_RESULT");

				while (!(team1Tuple == null && team2Tuple == null)
						|| firstTime) {

					if (lastTupleFetchedFromTeam1Sublist || firstTime) {
						team1Tuple = Utilities.getNextTuple(br1, relationName);
					}

					if (!lastTupleFetchedFromTeam1Sublist || firstTime) {
						team2Tuple = Utilities.getNextTuple(br2, relationName);
					}

					if (firstTime) firstTime = false;
							
					if ( (team1Tuple != null && team2Tuple != null
							&& team1Tuple.getAttributeValue(a) <= team2Tuple.getAttributeValue(a))
							|| (team1Tuple != null && team2Tuple == null) ) {
						
						minTuple = team1Tuple;
						lastTupleFetchedFromTeam1Sublist = true;
					} else if ( (team1Tuple != null && team2Tuple != null
							&& team1Tuple.getAttributeValue(a) > team2Tuple.getAttributeValue(a))
							|| (team1Tuple == null && team2Tuple != null) ) {
						
						minTuple = team2Tuple;
						lastTupleFetchedFromTeam1Sublist = false;
					}
										
					if (team1Tuple != null || team2Tuple != null)
						Utilities.writeTupleToFile(bw, minTuple);
									
				}
				Utilities.closeBufferedReader(br1);
				Utilities.closeBufferedReader(br2);
				Utilities.closeBufferedWriter(bw);

				Utilities.renameFile(tempDir + "/" + relationName + "_MERGE_RESULT", tempDir + "/" + team1Sublist);
				
				Utilities.deleteFile(tempDir + "/" + team2Sublist);
				
				team1SublistCounter++;
				team2SublistCounter++;
				
				System.out.println("[OK]");
			}
			
			number_of_sublists = (int) Math.ceil((double) number_of_sublists / 2);
			sublistSize = sublistSize * 2;
		}
		Utilities.renameFile(tempDir + "/" + relationName + "Sublist0", tempDir + "/" + sortedRelationCSV);
		
	}
	*/
	
	
	// Slower merge algorithm.
	// Reads one tuple at a time from each sorted sublist of the relation
	// and writes the min in the sortedRcsv or sortedScsv file.
	/*
	public void mergeAlternative(String csvfile, String relationName,
			int a, int m, String tempDir, String sortedRelationCSV) {
		
		int total_records = Utilities.getNumberOfRecords(csvfile);
		
		int number_of_sublists = (int) Math.ceil((double) total_records / m);
		
		System.out.println("relation " + relationName + " number of sublists: " + number_of_sublists);
		System.out.println();
		
		// Merge the sorted sublist files into one sorted file.
		
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
				System.out.print("Deleting file "
						+ relationName + "Sublist" + sublistIndexToBeDeleted + "...");
				BufferedReader br = brs.get(sublistIndexToBeDeleted);
				Utilities.closeBufferedReader(br);
				Utilities.deleteFile(tempDir + "/" + relationName + "Sublist" + sublistIndexToBeDeleted);
				System.out.println("[OK]");
			}
			if (minTuple != null) {
				sublistIndexWhereTheLastTupleWasFetchedFrom = minPos;
				Utilities.writeTupleToFile(bw, minTuple);
			}
		}

		Utilities.closeBufferedWriter(bw);
		
	}
	*/
	
	
	/** This function gets as an input two sorted relation files. **/
	public void join(String csvfile1, String csvfile2, int a1, int a2, int m, String outputFile) {
		
		String relation1Name = Utilities.getCSVName(csvfile1).replace("sorted", "");
		String relation2Name = Utilities.getCSVName(csvfile2).replace("sorted", "");
		
		BufferedReader br1 = Utilities.createBufferedReader(csvfile1);
		BufferedReader br2 = Utilities.createBufferedReader(csvfile2);
		BufferedWriter bw = Utilities.createBufferedWriter(outputFile);
		
		Tuple r = Utilities.getNextTuple(br1, relation1Name);
		Tuple s = Utilities.getNextTuple(br2, relation2Name);
		
		
		do {
			
			if (r.getAttributeValue(a1) == s.getAttributeValue(a2)) {
				Tuple outputTuple = Utilities.joinTuples(r, s, a1, a2);
				
				// if this is the first tuple to be written,
				// we should write the attributes header beforehand 
				if (Utilities.isEmptyFile(outputFile)) {
					Utilities.writeHeaderToFile(bw, outputTuple);
				}
				
				Utilities.writeTupleToFile(bw, outputTuple);
				
				Tuple tempS = s;
				
				// Output further tuples that match with r on attribute a1.
				// In case we have more than (m-2) that match with the same value on attribute a1,
				// the algorithm does not produce correct results.
				Utilities.markBufferedReader(br2, m);
				while ((s = Utilities.getNextTuple(br2, relation2Name)) != null &&
						(r.getAttributeValue(a1) == s.getAttributeValue(a2))) {
					
					outputTuple = Utilities.joinTuples(r, s, a1, a2);
					Utilities.writeTupleToFile(bw, outputTuple);
				}
				
				Utilities.resetBufferedReader(br2);
				
				// Output further tuples that match with s on attribute a2.
				// In case we have more than (m-2) that match with the same value on attribute a2,
				// the algorithm does not produce correct results.
				Utilities.markBufferedReader(br1, m);
				while ((r = Utilities.getNextTuple(br1, relation1Name)) != null &&
						(r.getAttributeValue(a1) == tempS.getAttributeValue(a2))) {
					
					outputTuple = Utilities.joinTuples(r, s, a1, a2);
					Utilities.writeTupleToFile(bw, outputTuple);
				}
				
				Utilities.resetBufferedReader(br1);
				
				r = Utilities.getNextTuple(br1, relation1Name);
				s = Utilities.getNextTuple(br2, relation2Name);
			} else if (r.getAttributeValue(a1) < s.getAttributeValue(a2)) {
				r = Utilities.getNextTuple(br1, relation1Name);
			} else if (r.getAttributeValue(a1) > s.getAttributeValue(a2)) {
				s = Utilities.getNextTuple(br2, relation2Name);
			}
		} while (r != null && s!= null);
		
		Utilities.closeBufferedReader(br1);
		Utilities.closeBufferedReader(br2);
		Utilities.closeBufferedWriter(bw);
	}
	
	
	/** SMJ that does not use sublist files. **/
	/*
	public void sortJoin(Tuple[] R, Tuple[] S, int a1, int a2, String outputFile) {

		Arrays.sort(R, new TupleComparator(a1));
		Arrays.sort(S, new TupleComparator(a2));
		
		int PR = 0; // pointer for R
		int PS = 0; // pointer for S
		
		BufferedWriter bw = Utilities.createBufferedWriter(outputFile);
		
		do {
			
			if (R[PR].getAttributeValue(a1) == S[PS].getAttributeValue(a2)) {
				Tuple outputTuple = Utilities.joinTuples(R[PR], S[PS], a1, a2);
				
				// if this is the first tuple to be written,
				// we should write the attributes header beforehand 
				if (Utilities.isEmptyFile(outputFile)) {
					Utilities.writeHeaderToFile(bw, outputTuple);
				}
				
				Utilities.writeTupleToFile(bw, outputTuple);
				
				// output further tuples that match with R[PR].getAttributeValue(a1)
				int tempPS = PS + 1;
				while (tempPS < S.length && (R[PR].getAttributeValue(a1) == S[tempPS].getAttributeValue(a2))) {
					outputTuple = Utilities.joinTuples(R[PR], S[tempPS], a1, a2);
					Utilities.writeTupleToFile(bw, outputTuple);
					tempPS++;
				}
				
				// output further tuples that match with S[PS].getAttributeValue(a2)
				int tempPR = PR + 1;
				while (tempPR < R.length && (R[tempPR].getAttributeValue(a1) == S[PS].getAttributeValue(a2))) {
					outputTuple = Utilities.joinTuples(R[tempPR], S[PS], a1, a2);
					Utilities.writeTupleToFile(bw, outputTuple);
					tempPR++;
				}
				
				PR++;
				PS++;
			} else if (R[PR].getAttributeValue(a1) < S[PS].getAttributeValue(a2)) {
				PR++;
			} else if (R[PR].getAttributeValue(a1) > S[PS].getAttributeValue(a2)) {
				PS++;
			}
		} while (PR != R.length && PS != S.length);
	
		Utilities.closeBufferedWriter(bw);
	}
	*/
	
}
