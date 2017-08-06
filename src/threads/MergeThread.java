package threads;

import java.io.BufferedReader;
import java.io.BufferedWriter;

import project.Tuple;
import project.Utilities;

// This is the merging phase of the external sorting algorithm.
// Splits the sublists to 2 teams.
// Merges 2 sublists from each team in each iteration,
// dividing the total number of sublists by 2, 
// up until only one sublist exists.
public class MergeThread extends Thread {

	String csvfile;
	String relationName;
	int a;
	int m;
	String tempDir;
	String sortedRelationCSV;
	
	public MergeThread(String csvfile, String relationName,
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

		
		/*** Merge the sorted sublist files of the relation into one sorted file. ***/

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
				
				System.out.println("Thread " + this.getId() + ": " +
						"Merging sublist " + team1Sublist + " with " + team2Sublist + "...");
				
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
				
			}
			
			number_of_sublists = (int) Math.ceil((double) number_of_sublists / 2);
			sublistSize = sublistSize * 2;
		}
		Utilities.renameFile(tempDir + "/" + relationName + "Sublist0", tempDir + "/" + sortedRelationCSV);
	
	}

	
}
