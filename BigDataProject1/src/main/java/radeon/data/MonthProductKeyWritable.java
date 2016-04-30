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
	
	public String toString() {
		return this.month.toString() + "," + this.product.toString();
	}
}
