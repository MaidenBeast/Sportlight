package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class FoodGenerator {
	
	private ArrayList<String> foods;
	private String foodDelimiter = ",";
	
	public FoodGenerator(String fileName) throws IOException{
		this.foods = new ArrayList<String>();
		File name = new File(fileName);
		if (name.isFile()) {
			try {
				BufferedReader input = new BufferedReader(new FileReader(name));
				String text;
				while ((text = input.readLine()) != null)
					foods.add(text);
				input.close();
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}
	}
	
	public String getSingleFood(){
		//int index = new Random().nextInt(this.foods.size());
		int index = FoodGenerator.getLinearRandomNumber(this.foods.size());
        String single_food = this.foods.get(index-1);
        return single_food;
	}
	
	public String getMutipleInterest(int n){
		String multiple_foods = "";
		HashSet<String> hs = new HashSet<String>();
		for (int i = 0; i<n; i++)	
			hs.add(getSingleFood());
		for (String text: hs)
             multiple_foods = multiple_foods + text + this.foodDelimiter;
		
		//Forzatura: cancella la virgola alla fine
        return multiple_foods.substring(0, multiple_foods.length() - 1);
	}
	
	public void setDelimiter(String delim) {
		this.foodDelimiter = delim;
	}
	
	/* Taken from
	 * https://stackoverflow.com/questions/5969447/java-random-integer-with-non-uniform-distribution
	 */
	private static int getLinearRandomNumber(int maxSize){
        //Get a linearly multiplied random number
        int randomMultiplier = maxSize * (maxSize + 1) / 2;
        Random r=new Random();
        int randomInt = r.nextInt(randomMultiplier);

        //Linearly iterate through the possible values to find the correct one
        int linearRandomNumber = 0;
        for(int i=maxSize; randomInt >= 0; i--){
            randomInt -= i;
            linearRandomNumber++;
        }

        return linearRandomNumber;
    }

}
