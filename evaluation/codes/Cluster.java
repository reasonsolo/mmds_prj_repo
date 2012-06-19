package results.kmeans;

import java.util.*;


public class Cluster{
	private int totalNum;
	private int kindsNum;
	private double []value;
	private int id;
	
	Cluster (int t, int k)
	{
		this.totalNum=t;
		this.kindsNum=k;
		value = new double [k];
		id = -1;
		
		for ( int i = 0 ; i < k ; i++)
		{
			value[i] = 0;
		}
	}
	
	public void assignID(int id){
		this.id = id;
	}
	
	
	
}