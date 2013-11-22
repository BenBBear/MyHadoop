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
			
			
			TextRecord tr2 = new TextRecord();
			tr2.setSplit(",");
			tr2.reset(new Text("a,b,c,d"));
			assertTrue(tr2.getField(0).equals(new Text("a")));
			assertTrue(tr2.length==4);
			
			tr2.append("abcd");
			assertTrue(tr2.length==5);
			assertTrue(tr2.getField(4).equals(new Text("abcd")));
			
			assertTrue(tr2.getRecord().equals(new Text("a,b,c,d,abcd")));
			
//			TextRecord tr = new TextRecord();//创建对象
			tr.setSplit("`");//设置分隔符
			Text temp = new Text("abcd`acd");
			tr.reset(temp);//将其他的Text存入record进行打散
			Text t0 = tr.getField(0);//获取第0个Text并使用
			t0.set("b");
			assertTrue(tr.getRecord().equals(new Text("b`acd")));
			Text t1 = tr.getField(1);//获取第1个Text并使用
			tr.getField(0).set("a");//修改
			Text returnText = tr.getRecord();//组合为最终的Text
			assertTrue(returnText.equals(new Text("a`acd")));
//			tr.reset(other);//处理下一个text
			
			
			assertTrue(tr.find("a")==0);
			assertTrue(tr.find("acd")==1);
			assertTrue(tr.find(new Text("acd"))==1);
			
			TextRecord ttx = new TextRecord();
			int length = tr.fieldSize();
			for(int i=0;i<length;i++){
				ttx.append(new Text(tr.getField(i)));
			}
			
			Record rec = new Record();
			rec.setSplitChar('`');
			rec.setRecord("a,b,c,d,e");
			System.out.println( rec.getRecord() );
			StringBuilder sb = new StringBuilder();
			rec.appendTo(sb);
			System.out.println( sb.toString() );
			
			
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		
	}

}
