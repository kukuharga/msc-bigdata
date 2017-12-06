package uk.ac.qmul.type;

public class Athlete {
	private String name;
	private String sport;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSport() {
		return sport;
	}

	public void setSport(String sport) {
		this.sport = sport;
	}

//	@Override
//	public int hashCode() {
//		return 0;
//	}
//
//	@Override
//	public boolean equals(Object obj) {
//		if (obj instanceof String) {
//
//			return this.equals((String) obj);
//		}
//		return false;
//	}

}
