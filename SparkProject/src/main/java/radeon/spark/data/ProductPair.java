package radeon.spark.data;

import java.io.Serializable;

public class ProductPair implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String left;
	private String right;
	private Double support;
	private Double confidence;
	
	public ProductPair(String left, String right) {
		this.left = left;
		this.right = right;
	}

	public String getLeft() {
		return left;
	}

	public void setLeft(String left) {
		this.left = left;
	}

	public String getRight() {
		return right;
	}

	public void setRight(String right) {
		this.right = right;
	}

	public Double getSupport() {
		return support;
	}

	public void setSupport(Double support) {
		this.support = support;
	}

	public Double getConfidence() {
		return confidence;
	}

	public void setConfidence(Double confidence) {
		this.confidence = confidence;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		int leftDiscrim = 2;
		result = prime * result + ((left == null) ? 0 : left.hashCode()*leftDiscrim);
		result = prime * result + ((right == null) ? 0 : right.hashCode());
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
		ProductPair other = (ProductPair) obj;
		if (left == null) {
			if (other.left != null)
				return false;
		} else if (!left.equals(other.left))
			return false;
		if (right == null) {
			if (other.right != null)
				return false;
		} else if (!right.equals(other.right))
			return false;
		return true;
	}

	public String toString() {
		return this.left + "," + this.right + "," + this.support + "%," + this.confidence + "%";
	}
}
