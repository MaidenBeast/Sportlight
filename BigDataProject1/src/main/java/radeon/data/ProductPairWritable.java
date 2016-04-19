package radeon.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ProductPairWritable implements Writable {
	
	private Text leftFood;
	private Text rightFood;
	private IntWritable supportCount;
	
	public ProductPairWritable(Text lf, Text rf) {
		this.leftFood = lf;
		this.rightFood = rf;
		this.supportCount = new IntWritable(0);
	}
	
	public Text getLeftFood() {
		return leftFood;
	}

	public void setLeftFood(Text leftFood) {
		this.leftFood = leftFood;
	}

	public Text getRightFood() {
		return rightFood;
	}

	public void setRightFood(Text rightFood) {
		this.rightFood = rightFood;
	}

	public IntWritable getSupportCount() {
		return supportCount;
	}
	
	public int getSupportCountAsInt() {
		return supportCount.get();
	}

	public void setSupportCount(IntWritable supportCount) {
		this.supportCount = supportCount;
	}

	public void increaseCount() {
		int newVal = this.getSupportCountAsInt() + 1;
		this.supportCount = new IntWritable(newVal);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.leftFood.readFields(in);
		this.rightFood.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.leftFood.write(out);
		this.rightFood.write(out);
	}
	
	public String toString() {
		return this.leftFood + "," + this.rightFood;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		//Costante moltiplicativa che permette di generare hashCode diversi se i membri della regola sono scambiati
		final int orderDiscrim = 2;
		int result = 1;
		result = prime * result
				+ ((leftFood == null) ? 0 : orderDiscrim*leftFood.hashCode());
		result = prime * result
				+ ((rightFood == null) ? 0 : rightFood.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProductPairWritable other = (ProductPairWritable) obj;
		if (leftFood == null) {
			if (other.leftFood != null)
				return false;
		} else if (!leftFood.equals(other.leftFood))
			return false;
		if (rightFood == null) {
			if (other.rightFood != null)
				return false;
		} else if (!rightFood.equals(other.rightFood))
			return false;
		return true;
	}
	
	
}
