package results.kmeans;

import java.util.*;

public class Main{

	public static void main(String[] args)
	{
		int kindsNum = Integer.parseInt(args[0]);
		System.out.println(kindsNum);
		ArrayList<Text> text = new ArrayList<Text>();
		double []purity = new double [kindsNum];
		for (int i = 0 ; i < kindsNum ; i++){
			Text tmp = new Text(args[i+1],kindsNum);
			text.add(tmp);			
		}
		
		for(int i=0; i< kindsNum; i++){
			System.out.println("text[1] :"+ text.get(1).getKindWeight(i));
		}
		
		double []maxW = new double [kindsNum];
		int []index = new int [kindsNum];
		int tmp;
		boolean flag=false;
		
		while(!flag)
		{
			flag =true;
			for ( int i=0; i< kindsNum; i++){
				index[i] = text.get(i).getMaxWeightIndex(); //最大权值的id
				maxW [i] = text.get(i).getKindWeight(index[i]);	// 最大权值的值
			}

			for(int i=0; i< kindsNum; i++){
				System.out.println("maxW["+ i + "] :"+ maxW[i]);
			}
			tmp = 0;
			for ( int i=0; i< kindsNum; i++){
				if(maxW[i]>maxW[tmp]) {
				tmp = i;
				}
			}
		
			text.get(tmp).setID(index[tmp]);
			purity[tmp] = maxW[tmp]; 
			text.get(tmp).setWeightToZero();
		
			for ( int i=0; i< kindsNum; i++){
				text.get(i).setKindWeightToZero(index[tmp]);
			}
			
			for( int i=0; i< kindsNum; i++){
				if(text.get(i).getID()==-1) flag=false;
			}
			
			for(int i=0; i< kindsNum; i++){
				System.out.println("Test["+ i + "].ID :"+ text.get(i).getID());
			}
		
		}
		
		for(int i=0; i< kindsNum; i++){
			System.out.println("Purity["+ i + "] :"+ purity[i]);
		}
		
	}
	
	
}