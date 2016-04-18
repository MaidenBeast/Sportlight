package radeon.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MonthWritable implements Writable, WritableComparable<MonthWritable> {
	
	private Text month;
	private IntWritable sum;
	
	public MonthWritable() {
		this.month = new Text();
		this.sum = new IntWritable();
	}
	
	public MonthWritable(Text month, IntWritable sum) {
		this.month = month;
		this.sum = sum;
	}
	
	public Text getMonth() {
		return month;
	}

	public void setMonth(Text month) {
		this.month = month;
	}

	public IntWritable getSum() {
		return sum;
	}
	
	public int getSumAsInt() {
		return sum.get();
	}

	public void setSum(IntWritable sum) {
		this.sum = sum;
	}
	
	public void addToSum(int add) {
		int res = this.getSumAsInt() + add;
		this.setSum(new IntWritable(res));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.month.readFields(in);
		this.sum.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.month.write(out);
		this.sum.write(out);
	}

	@Override
	public int compareTo(MonthWritable pw) {
		int cmp = this.sum.compareTo(pw.sum);
        if (cmp != 0) {
            return cmp;
        }
        return this.month.compareTo(pw.month);
	}
	
	public String toString() {
		return this.month.toString() + " " + this.sum.toString();
	}
}
