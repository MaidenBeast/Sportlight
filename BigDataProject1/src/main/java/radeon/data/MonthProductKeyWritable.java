package radeon.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import radeon.utils.MonthComparator;

public class MonthProductKeyWritable implements Writable, WritableComparable<MonthProductKeyWritable> {
	
	private Text month;
	private Text product;
	
	public MonthProductKeyWritable() {
		this.month = new Text();
		this.product = new Text();
	}
	
	public MonthProductKeyWritable(Text month, Text product) {
		this.month = month;
		this.product = product;
	}
	
	public Text getMonth() {
		return month;
	}

	public void setMonth(Text month) {
		this.month = month;
	}

	public Text getProduct() {
		return product;
	}

	public void setProduct(Text product) {
		this.product = product;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.month.readFields(in);
		this.product.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.month.write(out);
		this.product.write(out);
	}

	@Override
	public int compareTo(MonthProductKeyWritable mpkw) {
		MonthComparator comparator = new MonthComparator();
		int cmp = comparator.compare(this.month.toString(), mpkw.getMonth().toString());
        if (cmp != 0) {
            return cmp;
        }
        return this.product.compareTo(mpkw.getProduct());
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((month == null) ? 0 : month.hashCode());
		result = prime * result + ((product == null) ? 0 : product.hashCode());
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
		MonthProductKeyWritable other = (MonthProductKeyWritable) obj;
		if (month == null) {
			if (other.month != null)
				return false;
		} else if (!month.equals(other.month))
			return false;
		if (product == null) {
			if (other.product != null)
				return false;
		} else if (!product.equals(other.product))
			return false;
		return true;
	}

	public String toString() {
		return this.month.toString() + "," + this.product.toString();
	}
}
