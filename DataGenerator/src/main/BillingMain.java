package main;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import util.ItemBillingGenerator;

public class BillingMain {
	
	//Da usarsi specialmente per Hive, per delimitare gli elementi di un array
	//Delimitatore di default per l'elenco dei cibi: carattere virgola (,)
	private final static String FOOD_DELIMITER = "|";

	public static void main(String[] args) throws IOException {

		// Comando da usare per generare il dataset

		// prima bisogna configurare il generatore di scontrini
		/* al costruttore va passato un file che contiene in chiaro la lista di
		 * cibi da cui pescare
		 * 
		 * tale file è nella cartella billing
		 * il file food pu� tranquillamente essere editato aggiungendo nuovi
		 * cibi (uno per riga)
		 */

		Options options = new Options();

		options.addOption("f", "food-file", true, "Lista di cibi da cui pescare");
		options.addOption("o", "output-file", true, "Il dataset di output");
		options.addOption("r", "rows", true, "Il numero di righe del file");
		options.addOption("m", "max-food", true, "Il numero massimo di cibi per scontrino");

		final String PARSE_ERROR = "Usage: [-f|--food-file] <food-file> "
				+ "[-o|--output-file] <output-file> "
				+ "[-r|--rows] <rows> "
				+ "[[-m|--max-food] <max-food>]";

		CommandLineParser parser = new DefaultParser();
		CommandLine cmd;

		String foodFile = null;
		String outputFile = null;
		int rows = 0;
		int maxFood = 5; //default

		try {
			cmd = parser.parse(options, args);
			if (cmd.hasOption("f") && cmd.hasOption("o") && cmd.hasOption("r")) {
				foodFile = cmd.getOptionValue("f");
				outputFile = cmd.getOptionValue("o");
				rows = Integer.parseInt(cmd.getOptionValue("r"));
			} else {
				System.err.println(PARSE_ERROR);
				System.exit(1);
			}

			if (cmd.hasOption("m")) {
				maxFood = Integer.parseInt(cmd.getOptionValue("m"));
			}

			ItemBillingGenerator IB = new ItemBillingGenerator(foodFile);

			/* quindi bisogna richiamare la funzione generate in cui bisogna passare:
			 * - il nome del file in cui generare il dataset
			 * - il numero di righe del file (nell'esempio 10)
			 * - il numero massimo di cibi per scontrino (nell'esempio 5)
			 * - la data viene generata in modo randomico nel formato yyyy-mm-dd
			 */
			IB.generate(outputFile, rows, maxFood, FOOD_DELIMITER);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
