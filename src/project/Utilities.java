package project;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Utilities {
	
	
	// JOIN TWO TUPLES, PROJECT EVERY COLUMN ONCE (INCLUDING JOIN COLUMN ONCE)
	public static Tuple joinTuples(Tuple t1, Tuple t2, int a1, int a2) {
		Tuple joinTuple = new Tuple();
		int numberOfAttributesOfT1 = t1.getNumberOfAttributes();
		int numberOfAttributesOfT2 = t2.getNumberOfAttributes();
		joinTuple.setNumberOfAttributes(numberOfAttributesOfT1 + numberOfAttributesOfT2 - 1);
		
		int joinColumn = numberOfAttributesOfT1 - 1;
	
		int counter = 0;
		for (int i=0; i<numberOfAttributesOfT1; i++) {
			if (i != joinColumn) {
				if (i != a1) {
					joinTuple.attributes.add(t1.getAttribute(counter));
					counter++;
				}
				if (i == a1) {
					counter++;
					joinTuple.attributes.add(t1.getAttribute(counter));
					counter++;
				}
			}
			else if (i == joinColumn) {
				joinTuple.attributes.add(t1.getAttribute(a1));
				joinTuple.setAttributeName(joinColumn, t1.getRelationName() + a1
						+ "=" + t2.getRelationName() + a2);
			}
		}
		
		counter = numberOfAttributesOfT1;
		for (int i=numberOfAttributesOfT1; i<numberOfAttributesOfT1 + numberOfAttributesOfT2-1; i++) {
			if (i != joinColumn) {
				if (i-numberOfAttributesOfT1 != a2) {
					joinTuple.attributes.add(t2.getAttribute(counter-numberOfAttributesOfT1));
					counter++;
				}
				if (i-numberOfAttributesOfT1 == a2) {
					counter++;
					joinTuple.attributes.add(t2.getAttribute(counter-numberOfAttributesOfT1));
					counter++;
				}
			}
		}
		
		return joinTuple;
	}
	
	
	// Reads a ".csv" relation file and splits the data to a number of sublists.
	// Each sublist is sorted using quicksort and saved into a file.
	// It should contain m records.
	public static void splitCSVToSortedSublists(String csvfile, int a, int m, String tempDir) {
		System.out.println("Reading csv file: " + "\"" + csvfile + "\"" + "...");

		// first get the name of the relation from the csv file name
		File file = new File(csvfile);
		String relationName = file.getName().split(".csv")[0];
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(csvfile));
			
			String line = br.readLine(); // the first line contains the number of records
			int number_of_tuples = Integer.parseInt(line);
			
			int tupleCounter = 0;
			int sublistCounter = 0;
			List<Tuple> sublist = null;
			line = br.readLine();
			while(line != null) {
				if (tupleCounter % m == 0) {
					sublist = new ArrayList<Tuple>();
				}
				
				String[] attributes = line.split(",");
				Tuple tuple = new Tuple(attributes.length, relationName);
				
				for (int i=0; i<attributes.length; i++) {
					int attributeValue = Integer.parseInt(attributes[i]);
					String attributeName = relationName + i;
					Attribute attribute = new Attribute(attributeValue, attributeName);
					tuple.attributes.add(attribute);
				}
				
				// After parsing the tuple line into a Tuple object,
				// if the number of tuples in a sublist are m, write sublist to file. 
				sublist.add(tuple);
				tupleCounter++;
				String sublistFileName = tempDir + "/" + relationName + "Sublist" + sublistCounter;
				if ((tupleCounter % m == 0) || (tupleCounter == number_of_tuples)) {
					
					// sort the array using quicksort
					Collections.sort(sublist, new TupleComparator(a));
					writeRelationToFile(sublist, sublistFileName);
					sublistCounter++;
				}
				
				line = br.readLine();
			}
			
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	// Does almost the same operation as the method "splitCSVToSublists".
	// Only difference is that it creates a duplicate for each sublist file.
	// This method is used, in case we want to execute a join of a relation with itself.
	// Each sublist file has the following naming pattern: relationName + "1" + "Sublist" + sublistNumber.
	// Each duplicate sublist file has the following naming pattern: relationName + "2" + "Sublist" + sublistNumber.
	public static void splitCSVToDuplicateSortedSublists(String csvfile, int a1, int a2, int m, String tempDir) {
		System.out.println("Reading csv file: " + "\"" + csvfile + "\"" + "...");

		// first get the name of the relation from the csv file name
		File file = new File(csvfile);
		String relationName = file.getName().split(".csv")[0];
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(csvfile));
			
			String line = br.readLine(); // the first line contains the number of records
			int number_of_tuples = Integer.parseInt(line);
			
			int tupleCounter = 0;
			int relationCounter = 0;
			List<Tuple> sublist = null;
			line = br.readLine();
			while(line != null) {
				if (tupleCounter % (m) == 0) {
					sublist = new ArrayList<Tuple>();
				}
				
				String[] attributes = line.split(",");
				Tuple tuple = new Tuple(attributes.length, relationName);
				
				for (int i=0; i<attributes.length; i++) {
					int attributeValue = Integer.parseInt(attributes[i]);
					String attributeName = relationName + i;
					Attribute attribute = new Attribute(attributeValue, attributeName);
					tuple.attributes.add(attribute);
				}
				
				sublist.add(tuple);
				tupleCounter++;
				String sublistFile1Name = tempDir + "/" + relationName + "1" + "Sublist" + relationCounter;
				String sublistFile2Name = tempDir + "/" + relationName + "2" + "Sublist" + relationCounter;
				if ((tupleCounter % m == 0) || (tupleCounter == number_of_tuples)) {
					Collections.sort(sublist, new TupleComparator(a1));
					writeRelationToFile(sublist, sublistFile1Name);
					Collections.sort(sublist, new TupleComparator(a2));
					writeRelationToFile(sublist, sublistFile2Name);
					relationCounter++;
				}
				
				line = br.readLine();
			}
			
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	// Reads a csv file that represents a relation and returns the number of records.
	// The first line of the csv file should contain the number of records.
	public static int getNumberOfRecords(String csvfile) {
		int number_of_records = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(csvfile));
			String line = br.readLine();
			number_of_records = Integer.parseInt(line);
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return number_of_records;
	}

	
	public static int countLinesInAFile(String filename) {
		int lines = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			while (br.readLine() != null) {
				lines++;
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}
	
	
	public static boolean isEmptyFile(String filename) {
		boolean empty = true;
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			if (br.readLine() != null) {
				empty = false;
			}
			br.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return empty;
	}
	
	
	public static boolean fileExists(String filepath) {
		File file = new File(filepath);
		if(file.exists()) {
			return true;
		}
		return false;
	}
	
	
	public static void createTempDir(String tempDir) {
		File tempDirFile = new File(tempDir);
		tempDirFile.mkdirs();
	}
	
	
	public static void deleteTempDir(String tempDir) {
		File tempDirFile = new File(tempDir);
		
		String[] fileNames = tempDirFile.list();
		for(String fileName: fileNames){
		    File currentFile = new File(tempDirFile.getPath(), fileName);
		    currentFile.delete();
		}
		
		tempDirFile.delete();
	}
	
	
	public static void deleteFile(String file) {
		Path filepath = Paths.get(file);
		try {
			Files.deleteIfExists(filepath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void renameFile(String filepath, String newpath) {
		Path source = Paths.get(filepath);
		Path newdir = Paths.get(newpath);
		try {
			Files.move(source, newdir,
					StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	// deletes the result file if exists, from a previous run
	public static void createNewFile(String newFile) {
		File outputResultFile = new File(newFile);
		try {
			// first, delete the file if already exists from another run
			outputResultFile.delete();
			// then create the file
			outputResultFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void writeRelationToFile(List<Tuple> relation, String outputFile) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
			for (int i=0; i<relation.size(); i++) {
				bw.write(relation.get(i).getCSVRepresentation() + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void writeHeaderToFile(BufferedWriter bw, Tuple tuple) {
		try {
			bw.write(tuple.getHeader() + "\n");
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void writeTupleToFile(BufferedWriter bw, Tuple tuple) {
		try {
			// writer that appends to the file
			bw.write(tuple.getCSVRepresentation() + "\n");
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static BufferedReader createBufferedReader(String csvfile) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(csvfile));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return br;
	}
	
	
	public static BufferedReader createBufferedReaderAndSkipFirstLine(String csvfile) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(csvfile));
			br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return br;
	}
	
	
	public static void closeBufferedReader(BufferedReader br) {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	
	
	
	public static Tuple getNextTuple(BufferedReader br, String relationName) {
		Tuple tuple = null;
		try {
			String line = br.readLine();
			if (line != null) {
				String[] attributes = line.split(",");
				tuple = new Tuple(attributes.length, relationName);
				
				for (int i=0; i<attributes.length; i++) {
					int attributeValue = Integer.parseInt(attributes[i]);
					String attributeName = relationName + i;
					Attribute attribute = new Attribute(attributeValue, attributeName);
					tuple.attributes.add(attribute);
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return tuple;
	}
	
	
	public static void markBufferedReader(BufferedReader br, int memory) {
		// mark a pushback point
		try {
			// 20 characters is approximately the number of characters in every row
			// which can be considered as the tuple size.
			// Leave out two buffers for the tuples r & s.
			br.mark(20 * (memory-2));
//			System.out.println("BufferReader marked!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void resetBufferedReader(BufferedReader br) {
		try {
			br.reset();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static BufferedWriter createBufferedWriter(String csvfile) {
		BufferedWriter bw = null;
		try {
			// writer that appends to the file
			bw = new BufferedWriter(new FileWriter(csvfile, true));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bw;
	}
	
	
	public static void closeBufferedWriter(BufferedWriter bw) {
		try {
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static String getCSVName(String csvfile) {
		File file = new File(csvfile);
		return file.getName().split(".csv")[0];
	}
	
	
	public static void printRelation(List<Tuple> relation) {
		System.out.println(relation.get(0).getHeader());
		for (int i=0; i<relation.size(); i++) {
			System.out.println(relation.get(i));
		}
		System.out.println();
	}
		
		
}

