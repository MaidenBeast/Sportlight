package radeon.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//IMPLEMENTARE IL COSTRUTTORE NO-ARG, se non c'è crea problemi ad Hadoop
//Questo tipo è usato come chiave, quindi rientra nello shuffle and sort ed è richiesto quindi che implementi WritableComparable.
public class ProductPairWritable implements Writable, WritableComparable<ProductPairWritable> {
	
	private Text leftFood;
	private Text rightFood;
	
	public ProductPairWritable() {}
	
	public ProductPairWritable(Text lf, Text rf) {
		this.leftFood = lf;
		this.rightFood = rf;
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
	public int compareTo(ProductPairWritable o) {
		int cmp = this.getLeftFood().compareTo(o.getLeftFood());
		if (cmp == 0)
			return this.getRightFood().compareTo(o.getRightFood());
		return cmp;
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
