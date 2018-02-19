package cs522.project;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	public static final String WILD_CARD = "*";
	private String first;
	private String second;
	
	public Pair(String first, String second) {
		super();
		this.first = first;
		this.second = second;
	}
	
	public Pair() {
		first = "";
		second = "";
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readUTF();
		second = in.readUTF();		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeUTF(second);		
	}

	@Override
	public int compareTo(Pair o) {
		int result = first.compareTo(o.first);
		if (result == 0) {
			if (WILD_CARD.equals(second) && !WILD_CARD.equals(o.second)) {
				return -1;
			}
			if (!WILD_CARD.equals(second) && WILD_CARD.equals(o.second)) {
				return 1;
			}
			return second.compareTo(o.second);
		}
		return result;
	}
	
	@Override
	public String toString() {
		return "(" + first + "," + second + ")";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair)) {
			return false;
		}
		Pair other = (Pair) obj;
		return ((first == null && other.first == null) || (first != null && first.equals(other.first))) && 
				((second == null && other.second == null) || (second != null && second.equals(other.second)));

	}
	
	@Override
	public int hashCode() {		
		return Objects.hash(first, second);
	}
}
