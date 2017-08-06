package create_random_relations;

import java.util.Random;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class CreateRandomRelations {
	
	
	public static int N = 150;  // number of rows
	public static int K = 4;	// number of columns
	public static String relationFilepath = "my_relations/A.csv";
	
	
	public static void main (String[] args) {
		BufferedWriter bw = openBufferedWriter(relationFilepath);
		String firstLine = N + "\n";
		writeLine(bw, firstLine);
		
		Random r = new Random();
				
		int  maximum = 99999;
		int minimum = 10;
		
		for (int i=0; i<N; i++) {
		
			for (int j=0; j<K; j++) {
				int n = maximum - minimum + 1;
				int randomInt = Math.abs(r.nextInt() % n) + minimum;
				if (j != K-1) {
					String line = randomInt + ",";
					System.out.print(line);
					writeLine(bw, line);
				} else {
					String line = randomInt + "\n";
					System.out.print(line);
					writeLine(bw, line);
				}
			}
		
		}
		
		closeBufferedWriter(bw);
		
	}
	
	
	public static BufferedWriter openBufferedWriter(String filepath) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(filepath));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bw;
	}
	
	
	public static void writeLine(BufferedWriter bw, String line) {
		try {
			bw.write(line);
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void closeBufferedWriter(BufferedWriter bw) {
		try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
}
