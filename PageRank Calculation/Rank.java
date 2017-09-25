package pagerank;

public class Rank {
	
	String title;
	double rank;
	
	public Rank(String title, double rank) {
		super();
		this.title = title;
		this.rank = rank;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public double getRank() {
		return rank;
	}
	public void setRank(double rank) {
		this.rank = rank;
	}
	

}
