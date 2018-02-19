package cs522.project;

public class Pair {
	private int sum;
	private int count;
	
	public Pair(int sum, int count) {
		super();
		this.sum = sum;
		this.count = count;
	}
	public int getSum() {
		return sum;
	}
	public int getCount() {
		return count;
	}
	public void addCount(int count) {
		this.count += count;
	}
	public void addSum(int sum) {
		this.sum += sum;
	}
	
}
