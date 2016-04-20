package radeon.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ProductWritable implements Writable, WritableComparable<ProductWritable> {
	
	private Text name;
	private IntWritable count;
	
	public ProductWritable() {
		this.name = new Text();
		this.count = new IntWritable();
	}
	
	public ProductWritable(Text name, IntWritable count) {
		this.name = name;
		this.count = count;
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public IntWritable getCount() {
		return count;
	}
	
	public int getCountAsInt() {
		return count.get();
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name.readFields(in);
		this.count.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.name.write(out);
		this.count.write(out);
	}

	@Override
	public int compareTo(ProductWritable pw) {
		int cmp = this.count.compareTo(pw.count);
        if (cmp != 0) {
            return cmp;
        }
        return this.name.compareTo(pw.name);
	}
	
	public String toString() {
		return this.name.toString() + " " + this.count.toString();
	}
}
