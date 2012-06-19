package results.kmeans;

import java.util.*;
import java.io.*;


public class Text{
	private String name;
	private int [] value; 
	private int kindsNum;
	private int size;
	private double [] weight;
	private int id;
	
	Text ( String fileName, int kindsNum){
		this.kindsNum = kindsNum;
		this.id = -1;
		this.name = fileName;
		this.value = new int [this.kindsNum];
		for(int i=0; i<kindsNum ; i++)
		{
			this.value[i]=0;
		}
		
		File file = new File("src/results/kmeans/"+ fileName);
		BufferedReader reader = null;
        int line = 1;
        int tmp;
		try {
           // System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
               // System.out.println("line " + line + ": " + tempString);
            	tmp=Integer.parseInt(tempString);
            	this.value [tmp] ++;
            	line++;     	
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        size = line - 1;
        
        weight = new double [this.kindsNum];
        
        for ( int i=0 ;i<kindsNum;i++){
        	weight[i]=(double) (this.value[i]) / size;
        }
              
	}
	
    public int getSize(){
    	return this.size;
    }
    
    public double getKindWeight(int i){
    	return this.weight[i];
    }
    
    public void setID(int i)
    {
    	this.id = i;
    }
    
    public int getMaxWeightIndex()
    {
    	int maxi=0;
    	for ( int i=0; i< this.kindsNum; i++){
    		if(this.weight[i]>this.weight[maxi]){
    			maxi=i;
    		}   		
    	}
    	return maxi;
    }
    
    public void setWeightToZero(){
    	for ( int i=0; i< this.kindsNum; i++){
    		this.weight[i]=0;
    		
    	}
    }
    
    public void setKindWeightToZero(int i){
    	this.weight[i]=0;
    }
    
    public int getID(){
    	return this.id;
    }
}