package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;

//Preleva i primi n record da un file generato con BillingMain
//Necessario ai fini statistici per estrarre porzioni della stessa distribuzione, in caso contrario i dati statistici potrebbero risultare fra loro
//inconsistenti.
public class TakeTopMain {

	public static void main(String[] args) throws Exception {
		int howMany = Integer.parseInt(args[0]);
		String fileInput = args[1];
		String fileOutput = args[2];
		
		BufferedReader br = new BufferedReader(new FileReader(fileInput));
		PrintStream ps = new PrintStream(fileOutput);
		for (int i = 0; i < howMany ; i +=1) {
			String record = br.readLine();
			ps.println(record);
		}
		br.close();
		ps.close();
		System.out.println("Esecuzione terminata");
	}
}
