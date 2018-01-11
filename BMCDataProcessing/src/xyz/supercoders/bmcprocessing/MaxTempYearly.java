package xyz.supercoders.bmcprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class MaxTempYearly {
	public static void main(String[] args) throws IOException {
		
		HashMap<Integer, Float> map = new HashMap<>();
		File dir = new File("/home/bloombench/Documents/Ekta-Hadoop/hadoop-kit/data/weather_data_input");
		File[] files = dir.listFiles();
		for(File file : files){
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line ;//= br.readLine();
			while((line=br.readLine())!=null){
				
				String[] tokens = line.split("\\|");
				if(tokens.length == 5){
					int year = Integer.parseInt(tokens[1]);
					float temp = Float.parseFloat(tokens[4]);
					
					if(!map.containsKey(year)){
						map.put(year, temp);
					}
					else{
						float mapTemp = map.get(year);
						if(temp>mapTemp){
							map.put(year, temp);
						}
					}
				}
				//line = br.readLine();
			}
		}
		/*Set<Integer> keySet = map.keySet();
		for(Integer i : keySet){
			System.out.println(i+" "+map.get(i));
		}*/
		
		for(Entry<Integer, Float> e : map.entrySet()){
			System.out.println(e);
		}
	}
}
