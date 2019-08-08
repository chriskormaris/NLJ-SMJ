package multiple_merge_threads;

import java.util.ArrayList;
import java.util.List;

public class MergeIterationThread extends Thread {
	
	int a;
	int number_of_sublists;
	String relationName;
	String tempDir;
	
	public MergeIterationThread() {
		
	}
	
	public MergeIterationThread(int a, int number_of_sublists, String relationName, String tempDir) {
		this.a = a;
		this.number_of_sublists = number_of_sublists;
		this.relationName = relationName;
		this.tempDir = tempDir;
	}
	
	
	@Override
	public void run() {
		System.out.println("Thread " + this.getId() + ": " +
				"Merging " + number_of_sublists + " sublists for relation " + relationName + "!");
		
		int team1SublistCounter = 0;
		int team2SublistCounter = (int) Math.ceil(((double) number_of_sublists / 2));
		
		int number_of_team1_sublists = team2SublistCounter;
		int number_of_team2_sublists = number_of_sublists - number_of_team1_sublists;
		
		System.out.println();
		
		// Sometimes team 1 may have one more sublist than team 2.
		// In that case we do nothing for the last team1Sublist
		// in the current iteration.
		
		List<Thread> sublistThreads = new ArrayList<Thread>();
		for (int i=0; i<number_of_team2_sublists; i++) { // iterate floor(T1sublists/2) times
			
			Thread sublistThread = new SublistMergeThread(this.getId(), a, relationName, team1SublistCounter, 
					team2SublistCounter, tempDir);
			sublistThreads.add(sublistThread);
			sublistThread.start();
			
			team1SublistCounter++;
			team2SublistCounter++;
		}
		
		// Wait for all sub-threads to finish.
		for (Thread sublistThread: sublistThreads) {
			try {
				sublistThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

}
