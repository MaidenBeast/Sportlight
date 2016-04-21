package radeon.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Formatter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class PercentagesWritable implements Writable {
	
	private DoubleWritable support;
	private DoubleWritable confidence;
	
	public PercentagesWritable() {
		this.support = new DoubleWritable();
		this.confidence = new DoubleWritable();
	}
	
	public PercentagesWritable(DoubleWritable support, DoubleWritable confidence) {
		this.support = support;
		this.confidence = confidence;
	}
	
	public DoubleWritable getSupport() {
		return support;
	}

	public void setSupport(DoubleWritable support) {
		this.support = support;
	}

	public DoubleWritable getConfidence() {
		return confidence;
	}

	public void setConfidence(DoubleWritable confidence) {
		this.confidence = confidence;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.support.readFields(in);
		this.confidence.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this.support.write(out);
		this.confidence.write(out);
	}
	
	public String toString() {
		double dblSupport = this.support.get()*100;
		double dblConfidence = this.confidence.get()*100;
		
		String strSupport = String.format("%.2f", dblSupport);
		String strConfidence = String.format("%.2f", dblConfidence);
		
		return strSupport + "%, " + strConfidence + "%";
	}
}
