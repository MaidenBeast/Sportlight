package it.uniroma3.bigdata.radeon.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class ProductWritableList implements Writable {
	
	private List<ProductWritable> products;
	
	public ProductWritableList() {
		this.products = new ArrayList<>();
	}
	
	public ProductWritableList(List<ProductWritable> products) {
		this.products = products;
	}
	
	public void addProduct(ProductWritable prod) {
		this.products.add(prod);
	}

	public List<ProductWritable> getProducts() {
		return products;
	}
	
	public void setProducts(List<ProductWritable> products) {
		this.products = products;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			while(true) {
				ProductWritable prod = new ProductWritable();
				prod.readFields(in);
				this.addProduct(prod);
			}
		}
		catch (EOFException e) {
			return;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (ProductWritable prod : this.products) {
			prod.write(out);
		}
	}
}
