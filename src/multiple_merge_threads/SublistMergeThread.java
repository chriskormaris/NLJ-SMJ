package multiple_merge_threads;

import project.Tuple;
import project.Utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;

public class SublistMergeThread extends Thread {

	private final long parentThreadId;
	private final int a; // the tuple attribute index
	private final String relationName;
	private final int team1SublistCounter;
	private final int team2SublistCounter;
	private final String tempDir;

	public SublistMergeThread(
			long parentThreadId,
			int a,
			String relationName,
			int team1SublistCounter,
			int team2SublistCounter,
			String tempDir
	) {
		this.parentThreadId = parentThreadId;
		this.a = a;
		this.relationName = relationName;
		this.team1SublistCounter = team1SublistCounter;
		this.team2SublistCounter = team2SublistCounter;
		this.tempDir = tempDir;
	}

	@Override
	public void run() {
		Tuple minTuple = null;

		String team1Sublist = relationName + "Sublist" + team1SublistCounter;
		String team2Sublist = relationName + "Sublist" + team2SublistCounter;

		System.out.println("\tParent Thread id: " + this.parentThreadId + ", Thread id: " + this.getId() +
				", Merging sublist " + team1Sublist + " with " + team2Sublist + "...");

		Tuple team1Tuple = null;
		Tuple team2Tuple = null;

		boolean lastTupleFetchedFromTeam1Sublist = false;
		boolean firstTime = true;
		BufferedReader br1 = Utilities.createBufferedReader(tempDir + "/" + team1Sublist);
		BufferedReader br2 = Utilities.createBufferedReader(tempDir + "/" + team2Sublist);
		BufferedWriter bw = Utilities.createBufferedWriter(tempDir + "/" + team1Sublist + "_" + team2Sublist + "_MERGE_RESULT");

		while (!(team1Tuple == null && team2Tuple == null)
				|| firstTime) {

			if (lastTupleFetchedFromTeam1Sublist || firstTime) {
				team1Tuple = Utilities.getNextTuple(br1, relationName);
			}

			if (!lastTupleFetchedFromTeam1Sublist) {
				team2Tuple = Utilities.getNextTuple(br2, relationName);
			}

			if (firstTime) firstTime = false;

			if ( (team1Tuple != null && team2Tuple != null
					&& team1Tuple.getAttributeValue(a) <= team2Tuple.getAttributeValue(a))
					|| (team1Tuple != null && team2Tuple == null) ) {

				minTuple = team1Tuple;
				lastTupleFetchedFromTeam1Sublist = true;
			} else if (team1Tuple != null && team1Tuple.getAttributeValue(a) > team2Tuple.getAttributeValue(a)
					|| team1Tuple == null && team2Tuple != null) {

				minTuple = team2Tuple;
				lastTupleFetchedFromTeam1Sublist = false;
			}

			if (team1Tuple != null || team2Tuple != null)
				Utilities.writeTupleToFile(bw, minTuple);

		}
		Utilities.closeBufferedReader(br1);
		Utilities.closeBufferedReader(br2);
		Utilities.closeBufferedWriter(bw);

		Utilities.renameFile(tempDir + "/" + team1Sublist + "_" + team2Sublist + "_MERGE_RESULT", tempDir + "/" + team1Sublist);

		Utilities.deleteFile(tempDir + "/" + team2Sublist);
	}

}
