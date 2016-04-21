package radeon.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ProductPairCountWritable implements Writable {
	private ProductPairWritable productPair;
	private IntWritable supportCount;
	
	public ProductPairCountWritable() {
		this.productPair = new ProductPairWritable();
		this.supportCount = new IntWritable();
	}
	
	public ProductPairCountWritable(ProductPairWritable ppw, IntWritable count) {
		this.productPair = ppw;
		this.supportCount = count;
	}
	
	public ProductPairCountWritable(Text lf, Text rf, IntWritable count) {
		this.productPair = new ProductPairWritable(lf, rf);
		this.supportCount = count;
	}

	public ProductPairWritable getProductPair() {
		return productPair;
	}

	public void setProductPair(ProductPairWritable productPair) {
		this.productPair = productPair;
	}

	public IntWritable getSupportCount() {
		return supportCount;
	}

	public void setSupportCount(IntWritable supportCount) {
		this.supportCount = supportCount;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.productPair.readFields(in);
		this.supportCount.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.productPair.write(out);
		this.supportCount.write(out);
	}
	
	public String toString() {
		return this.productPair + " " +this.supportCount.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((productPair == null) ? 0 : productPair.hashCode());
		result = prime * result + ((supportCount == null) ? 0 : supportCount.hashCode());
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
		ProductPairCountWritable other = (ProductPairCountWritable) obj;
		if (productPair == null) {
			if (other.productPair != null)
				return false;
		} else if (!productPair.equals(other.productPair))
			return false;
		if (supportCount == null) {
			if (other.supportCount != null)
				return false;
		} else if (!supportCount.equals(other.supportCount))
			return false;
		return true;
	}
	
}
