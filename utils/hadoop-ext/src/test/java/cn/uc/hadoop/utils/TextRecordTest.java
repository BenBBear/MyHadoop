package cn.uc.hadoop.utils;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TextRecordTest {

	@Test
	public void test() {
		try{
			TextRecord tr = new TextRecord();
			tr.setSplit(",,");
			boolean catchE= false;
			try{
				tr.field(0);
			}
			catch(Exception e){
				catchE = true;
			}
			assertTrue(catchE);
			
			tr.append(new Text("1234"));
			tr.append("abcd");
			tr.append('真');
			assertTrue(tr.getRecord().compareTo(new Text("1234,,abcd,,真"))==0);
			
			tr.reset(new Text("哈哈,,ddd,,2323"));
			assertTrue(tr.fieldSize()==3);
			assertTrue(tr.getRecord().compareTo(new Text("哈哈,,ddd,,2323"))==0);
			
			tr.reset(new Text("哈哈,,ddd,,2323,,"));
			assertTrue(tr.fieldSize()==4);
			assertTrue(tr.getRecord().compareTo(new Text("哈哈,,ddd,,2323,,"))==0);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		
	}

}
