package util;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;

public class ItemBillingGenerator {

	private FoodGenerator ig;

	public ItemBillingGenerator(String food_file) throws IOException{
		this.ig = new FoodGenerator(food_file);
	}

	public void generate(String fileName, int num_items, int max_num_foods, String delim) {
		this.ig.setDelimiter(delim);
		this.generate(fileName, num_items, max_num_foods);
	}

	public void generate(String fileName, int num_items, int max_num_foods){

		try
		{
			//FileOutputStream stream = new FileOutputStream(fileName);
			BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(fileName));
			PrintStream data = new PrintStream(stream);
			for(int i=0;i<num_items;i++)
			{

				int num_foods = new Random().nextInt(max_num_foods);
				if (num_foods == 0) {
					num_foods += 1;
				}
				String date = RandomDateGenerator.generate();
				String foods = ig.getMutipleInterest(num_foods);
				data.println(date+","+foods);
			}
			data.close();
			stream.close();
		}
		catch (IOException e)
		{
			System.out.println("Error: " + e);
			System.exit(1);
		}


	}

}

