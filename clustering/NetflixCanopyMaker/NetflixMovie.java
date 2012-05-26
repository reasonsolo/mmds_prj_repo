//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.util.*;


public class NetflixMovie {

	public String movie_id = "";
	public ArrayList<Triple> features; 
	
	public NetflixMovie(String id, String data) {
		this.movie_id = id;
		String[] toAdd = data.split(";");
		this.features = new ArrayList<Triple>();
		for(String s: toAdd)
			this.features.add(new Triple(s));
	}

	
	// This returns the number of matching users.
	public int MatchCount(NetflixMovie movie) {
		Iterator<Triple> it1 = this.features.iterator();
		Iterator<Triple> it2 = movie.features.iterator();
		int matchCount = 0;
		Integer one = null;
		Integer two = null;
		while((it1.hasNext())&&(it2.hasNext())) {
			if((one==null)&&(two==null)) {
				one = it1.next().one;
				two = it2.next().one;
			}
			if(one.equals(two)) {
				matchCount += 1;
				one = it1.next().one;
				two = it2.next().one;
			} else if(one.compareTo(two)<0)
				one = it1.next().one;
			else
				two = it2.next().one;
		}
		return matchCount;
	}

	
	
	// This returns the number of matching users divided by the total number of users
	// reviewing the lesser-reviewed movie.
	public double SimpleDistance(NetflixMovie movie) {
		Iterator<Triple> it1 = this.features.iterator();
		Iterator<Triple> it2 = movie.features.iterator();
		int matchCount = 0;
		int totalCount = 1;
		Integer one = null;
		Integer two = null;
		while((it1.hasNext())&&(it2.hasNext())) {
			if((one==null)&&(two==null)) {
				one = it1.next().one;
				two = it2.next().one;
			}
			if(one.equals(two)) {
				matchCount += 1;
				one = it1.next().one;
				two = it2.next().one;
			} else if(one.compareTo(two)<0)
				one = it1.next().one;
			else
				two = it2.next().one;
			totalCount +=1;
		}
		return matchCount / (double)totalCount;
	}
	
	
	public double ComplexDistance(NetflixMovie movie) {
		return 0.0;
	}
	
	
	private class Triple {
		public Integer one;
		public String two;
		public Triple(String data) {
			String[] items = data.split(",");
			one = new Integer(items[0]);
			two = items[1];
		}
	}
}
