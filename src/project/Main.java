package project;

/**
 * Memory restriction implementation, using batch files.
 **/
public class Main {

	public static void main(String[] args) {

		/* PARSE ARGUMENTS */

		// args example: -f1 testdata/B.csv -a1 1 -f2 testdata/B.csv -a2 2 -j SMJ -m 200 -t tmp -o results.csv
		String csvfile1 = "";
		String csvfile2 = "";
		int a1 = 0;
		int a2 = 0;
		String joinAlgorithm = "";
		int memory = 0;
		String tempDir = "";
		String outputFile = "";

		try {
			// if join algorithm is NLJ the tempDir argument is not needed
			if (args.length >= 12) {
				for (int i = 0; i < args.length; i++) {
					if (args[i].equals("-f1")) {
						if (i == args.length - 1) {
							throw new Exception("Improper use of -f1. It should not be the last argument!");
						}
						csvfile1 = args[i + 1];
					}
					if (args[i].equals("-f2")) {
						if (i == args.length - 1) {
							throw new Exception("Improper use of -f2. It should not be the last argument!");
						}
						csvfile2 = args[i + 1];
					}
					if (args[i].equals("-a1")) {
						if (i == args.length - 1) {
							throw new Exception("Improper use of -a1. It should not be the last argument!");
						}
						a1 = Integer.parseInt(args[i + 1]);
					}
					if (args[i].equals("-a2")) {
						if (i == args.length - 1) {
							throw new Exception("Improper use of -a2. It should not be the last argument!");
						}
						a2 = Integer.parseInt(args[i + 1]);
					}
					if (args[i].equals("-j")) {
						if (i == args.length - 1) {
							throw new Exception("Improper use of -j. It should not be the last argument!");
						}
						joinAlgorithm = args[i + 1];
					}
					if (args[i].equals("-m")) {
						if (i == args.length - 1) {
							throw new Exception("Improper use of -m. It should not be the last argument!");
						}
						memory = Integer.parseInt(args[i + 1]);
					}
					if (args[i].equals("-t")) {
						if (i == args.length - 1) {
							throw new Exception("Improper use of -t. It should not be the last argument!");
						}
						tempDir = args[i + 1];
					}
					if (args[i].equals("-o")) {
						if (i == args.length - 1) {
							throw new Exception("Improper use of -o. It should not be the last argument!");
						}
						outputFile = args[i + 1];
					}
				}
			} else {
				throw new Exception("Very few arguments given!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}


		/* PARSE RELATION CSV FILES */

		// get the name of the 1st relation from the csv file name
		String relation1Name = Utilities.getCSVName(csvfile1);

		// get the name of the 2nd relation from the csv file name
		String relation2Name = Utilities.getCSVName(csvfile2);


		int T1 = Utilities.getNumberOfRecords(csvfile1); // number of records in relation1
		int T2 = Utilities.getNumberOfRecords(csvfile2); // number of records in relation2


		// System.out.println();

		System.out.println("relation " + relation1Name + " number of records: " + T1);
		System.out.println("relation " + relation2Name + " number of records: " + T2);


		System.out.println();

		/* RUN JOIN */

		if (joinAlgorithm.equalsIgnoreCase("smj")) {

			/* Sort-Merge Join */

			System.out.println("Running Sort-Merge Join...");
			SortMergeJoin smj = new SortMergeJoin();

			// Sort Merge Join R, S on a1=a2
			long smjStartTime = System.currentTimeMillis();
			smj.sortMergeJoin(csvfile1, csvfile2,
					a1, a2, memory, tempDir, outputFile);
			long smjTotalTime = System.currentTimeMillis() - smjStartTime;
			double smjTotalTimeInSec = (double) smjTotalTime / 1000;

			System.out.println();
			int number_of_join_records = Utilities.countLinesInAFile(outputFile) - 1;
			System.out.println("number of join records: " + number_of_join_records);

			System.out.println("SMJ total time: " + smjTotalTimeInSec + " sec");

			String smjComplexity = "5 *" + " ( " + "T(" + relation1Name + ") + " + "T(" + relation2Name + ") )";
			System.out.println("SMJ complexity: " + smjComplexity);

			double smjIOs = Math.ceil(5 * (T1 + T2));
			System.out.printf("SMJ Cost: %.0f I/Os ", smjIOs);
			System.out.println();

		} else if (joinAlgorithm.equalsIgnoreCase("nlj")) {

			/* Nested Loop Join */

			System.out.println("Running Nested Loop Join...");
			NestedLoopJoin nlj = new NestedLoopJoin();

			// Nested Loop Join R, S on a1=a2
			long nljStartTime = System.currentTimeMillis();
			// nlj.nestedLoopJoin(csvfile1, csvfile2, a1, a2, outputFile);
			nlj.blockedNestedLoopJoin(csvfile1, csvfile2, a1, a2, memory, outputFile);
			long nljTotalTime = System.currentTimeMillis() - nljStartTime;
			double nljTotalTimeInSec = (double) nljTotalTime / 1000;

			System.out.println();
			int number_of_join_records = Utilities.countLinesInAFile(outputFile) - 1;
			System.out.println("number of join records: " + number_of_join_records);

			System.out.println("NLJ total time: " + nljTotalTimeInSec + " sec");

			String nljComplexity = "T(" + relation1Name + ") * " + "T(" + relation2Name + ")";
			System.out.println("NLJ complexity: " + nljComplexity);

			double nljIOs = T1 * T2;
			System.out.printf("NLJ Cost: %.0f I/Os", nljIOs);
			System.out.println();
		}

		long memoryUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		int memoryUsedInMB = (int) ((double) memoryUsed / 1048576);
		System.out.println("Total memory used: " + memoryUsedInMB + " MB");
	}

}
