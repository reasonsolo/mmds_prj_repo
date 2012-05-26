//
// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
import java.util.*;


public class NetflixMovie {
	// This is the master version of the NetflixMovie class. Many copies exist in other map-reduces,
	// but this one is the most advanced.
	
	
	public String movie_id = "";
	public ArrayList<Triple> features; 
	
	public NetflixMovie(String id, String data) {
		this.movie_id = id;
		String[] toAdd = data.split(";");
		this.features = new ArrayList<Triple>();
		for(String s: toAdd)
			if(s.length()>1)
				this.features.add(new Triple(s));
	}

	// This returns the number of matching users.
	public int MatchCount(NetflixMovie movie, int thresh) {
		Iterator<Triple> it1 = this.features.iterator();
		Iterator<Triple> it2 = movie.features.iterator();
		int matchCount = 0;
		Integer one = null;
		Integer two = null;
		while((it1.hasNext())&&(it2.hasNext())) {
			if((one==null)&&(two==null)) {
				one = it1.next().userID;
				two = it2.next().userID;
				continue;
			}
			if(one.equals(two)) {
				matchCount += 1;
				one = it1.next().userID;
				two = it2.next().userID;
			} else if(one.compareTo(two)<0)
				one = it1.next().userID;
			else
				two = it2.next().userID;
			if(matchCount>thresh)
				break;
		}
		return matchCount;
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
				one = it1.next().userID;
				two = it2.next().userID;
			}
			if(one.equals(two)) {
				matchCount += 1;
				one = it1.next().userID;
				two = it2.next().userID;
			} else if(one.compareTo(two)<0)
				one = it1.next().userID;
			else
				two = it2.next().userID;
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
				one = new Integer(it1.next().userID);
				two = new Integer(it2.next().userID);
			}
			if(one.equals(two)) {
				matchCount += 1;
				one = new Integer(it1.next().userID);
				two = new Integer(it2.next().userID);
			} else if(one.compareTo(two)<0)
				one = new Integer(it1.next().userID);
			else
				two = new Integer(it2.next().userID);
			totalCount +=1;
		}
		return matchCount / (double)totalCount;
	}
	
	// uses cosine distance - actually returns the square of the cosine of the angle between the
	// two high dimensional vectors.
	// Closer to 1 means higher similarity.
	public double ComplexDistance(NetflixMovie movie) {
		Iterator<Triple> it1 = this.features.iterator();
		Iterator<Triple> it2 = movie.features.iterator();
		
		double dotProduct = 0.0;
		double magOne = 0.0;
		double magTwo = 0.0;
		Triple one = null;
		Triple two = null;
		while((it1.hasNext())&&(it2.hasNext())) {
			if((one==null)&&(two==null)) {
				one = it1.next();
				two = it2.next();
			}
			if((new Integer(one.userID)).equals(new Integer(two.userID))) {
				dotProduct += one.rating*two.rating;
				one = it1.next();
				two = it2.next();
			} else if((new Integer(one.userID)).compareTo(new Integer(two.userID))<0) {
				magOne += one.rating*one.rating;
				one = it1.next();
			}else {
				magTwo += two.rating*two.rating;
				two = it2.next();
			}
		}
		while(it1.hasNext()) {
			Triple t = it1.next();
			magOne += t.rating*t.rating;
		}
		while(it2.hasNext()) {
			Triple t = it2.next();
			magTwo += t.rating*t.rating;			
		}
		return (dotProduct*dotProduct) / (1+magOne*magTwo);
	}
	
	
	public class Triple {
		public int userID;
		public int rating;
		//public String three;
		public Triple(String data) {
			String[] items = data.split(",");
			userID = (new Integer(items[0])).intValue();
			rating  = (new Integer(items[1])).intValue();
			//three = items[2];
		}
	}
}
