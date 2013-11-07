package cn.uc.hadoop.mapreduce.lib.partition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestTextFirstPartitioner {

	@Test
	public void testHashValue() {
		
		TextFirstPartitioner part = new TextFirstPartitioner();
		part.setConf(new Configuration());
		int[] result = new int[100];
		HashSet set = new HashSet();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File("C:/Users/Administrator/Downloads/head_10000_data_x")));
			String line = null;
			while((line=br.readLine())!=null){
				int x = part.getPartition(new Text(line), new Text("haha"), 51);
				
				result[x]++;
				set.add(line);
			}
			for( int i=0;i<50;i++){
				System.out.println(i+" "+result[i]);
			}
			System.out.println(set.size());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(part.getPartition(new Text("fffffffffffa123c25d448468179f22929113999``dccdcd"), new Text("haha"), 12));
		
		
		
//		fail("Not yet implemented");
	}

}
