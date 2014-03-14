package com.autentia.tutoriales;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class MeasureWritable implements WritableComparable<MeasureWritable> {

	private String year;
	private String province;

	public MeasureWritable() {

	}

	public MeasureWritable(String year, String province) {
		this.year = year;
		this.province = province;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, year);
		Text.writeString(out, province);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = Text.readString(in);
		province = Text.readString(in);
	}

	public String getYear() {
		return this.year;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(province).append(year).toHashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof MeasureWritable)) {
			return false;
		}

		final MeasureWritable other = (MeasureWritable) o;
		return new EqualsBuilder().append(province, other.province).append(year, other.year).isEquals();
	}

	@Override
	public String toString() {
		return "(" + year + ") - " + province;
	}

	@Override
	public int compareTo(MeasureWritable measureWritable) {
		return new CompareToBuilder().append(this, measureWritable).toComparison();
	}

	public static class Comparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

		public Comparator() {
			super(MeasureWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	static {
		WritableComparator.define(MeasureWritable.class, new Comparator());
	}

}
