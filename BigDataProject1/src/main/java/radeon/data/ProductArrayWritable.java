package radeon.data;

import org.apache.hadoop.io.ArrayWritable;

public class ProductArrayWritable extends ArrayWritable {
	public ProductArrayWritable() {
		super(ProductWritable.class);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings())
        {
            sb.append(s).append(" ");
        }
        return sb.toString();
	}
}
