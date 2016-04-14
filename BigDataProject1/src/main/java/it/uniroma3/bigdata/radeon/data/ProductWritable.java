package it.uniroma3.bigdata.radeon.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ProductWritable implements WritableComparable<ProductWritable> {
	
	private String name;
	private int count;
	
	public ProductWritable() {}
	
	public ProductWritable(String name, int count) {
		this.name = name;
		this.count = count;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.name);
		out.writeInt(this.count);
	}

	@Override
	public int compareTo(ProductWritable o) {
		if (this.count < o.getCount()) {
			return -1;
		}
		else if (this.count > o.getCount()) {
			return 1;
		}
		return 0;
	}
}
