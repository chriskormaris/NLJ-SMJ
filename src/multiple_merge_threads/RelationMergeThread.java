package multiple_merge_threads;

import project.Utilities;

// This is the merging phase of the external sorting algorithm.
// Splits the sublists to 2 teams.
// Merges 2 sublists from each team in each iteration,
// dividing the total number of sublists by 2, 
// up until only one sublist exists.
// Visit the following url for further explanation:
// http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX1.htm
public class RelationMergeThread extends Thread {

	private final String csvfile;
	private final String relationName;
	private final int a;
	private final int m;
	private final String tempDir;
	private final String sortedRelationCSV;
	
	public RelationMergeThread(
			String csvfile,
			String relationName,
			int a,
			int m,
			String tempDir,
			String sortedRelationCSV
	) {
		
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
		
		/*** Merge the sorted sublist files of the relation into one sorted file. ***/

		// Starting from sublists of size m ,
		// each time we double the length until we reach size T1.
		int sublistSize = m;
		while (sublistSize <= total_records) { // merge iterations
			
			MergeIterationThread iterationThread = new MergeIterationThread(a, number_of_sublists, relationName, tempDir);
			iterationThread.run();
			
			number_of_sublists = (int) Math.ceil((double) number_of_sublists / 2);
			sublistSize = sublistSize * 2;
		}
		
		Utilities.renameFile(tempDir + "/" + relationName + "Sublist0", tempDir + "/" + sortedRelationCSV);
	
	}

	
}
