package cn.uc.hadoop.utils;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.nio.charset.CharacterCodingException;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import cn.uc.hadoop.exception.TextSplitIndexOutOfBoundsException;

public class TestTextUtils {
	private	String s = "真的不错啊abcd";
	private	String s1 = s;
	private	String s2 = "真的不错啊";
	private	String s3 = "abcd";
	private	char c = '真';
	private	char c1 = c;
	private	char c2 = 'a';
	 static abstract class SimpleBenchMark{
		protected abstract String getName();
		protected abstract String test1(int number);
		protected abstract String test2(int number);
		protected String test3(int number){
			return null;
		}
		public void runBeanchMark(int number){
			long begin = System.nanoTime();
			String t1 =test1(number);
			long end = System.nanoTime();
			long u1 = end-begin;
			begin = System.nanoTime();
			String t2 =test2(number);
			end = System.nanoTime();
			long u2 = end-begin;
			begin = System.nanoTime();
			String t3 =test3(number);
			end = System.nanoTime();
			long u3 = end-begin;
			System.out.println("testing:"+getName()+" for "+number+" times");
			System.out.println("" + t1 +" using "+ u1/1000 + " ms" );
			System.out.println("" + t2 +" using "+ u2/1000 + " ms" );
			if(t3!=null) System.out.println("" + t3 +" using "+ u3/1000 + " ms" );
			if( t3!=null && u3<u1 && u3<u2 ){
				System.out.println("resutl is :"+t3+" fastest .");
			}
			else{
				if(u1<u2) System.out.println("resutl is :"+t1+" faster ");
				else System.out.println("resutl is :"+t2+" faster ");
			}
			System.out.println(" ");
		}
	}
	private Text getLessByteText(String a){
		return getLessByteText(a,a);
	}
	private Text getLessByteText(String a,String more){
		Text text = new Text(a+more);
		text.set(a);
		return text;
	}
	private static String generateString(Random rng, String characters, int length)
	{
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = characters.charAt(rng.nextInt(characters.length()));
	    }
	    return new String(text);
	}
	@Test
	public void testEncode() {
		try {
			byte[] bs = TextUtils.encode(s);
			byte[] bs2 = TextUtils.encode(s2);
			byte[] bs3 = TextUtils.encode(s3);
			byte[] cs = TextUtils.encode(c);
			byte[] cs2 = TextUtils.encode(c2);

			String ts = new String(TextUtils.encode(s));
			assertTrue(ts.equals(s));
			// 测试 ‘真’ 这个字符是否对应
			boolean same = true;
			for (int i = 0; i < cs.length; i++) {
				if (bs[i] != cs[i]) {
					System.out.println(bs[i] + " " + cs[i]);
					same = false;
					break;
				}
			}
			assertTrue(same);
			//测试字节数是否相同
			assertTrue(bs.length==bs2.length+bs3.length);
			//测试跟string的getbyte相同
			byte[] temp = s.getBytes();
			assertTrue(temp.length==bs.length);
			same = true;
			for (int i = 0; i < bs.length; i++) {
				if (bs[i] != temp[i]) {
					System.out.println(bs[i] + " " + temp[i]);
					same = false;
					break;
				}
			}
			assertTrue(same);
			
			// 测试 ‘a’ 这个字符是否在s2之后
			assertTrue(cs2.length == 1);
			assertTrue(bs[bs2.length] == cs2[0]);

		} catch (Exception e) {
			assertTrue(false);
			e.printStackTrace();
		}

	}
	@Test
	public void testAppend() {
		

		try {
			Text t1 = new Text(s);
			TextUtils.append(t1,s,s2,s3);
			
			Text t2 = getLessByteText(s);
			TextUtils.append(t2,s,s2,s3);
			
			byte[] x = (s+s+s2+s3).getBytes();
			Text t3 = new Text(x);
			
			assertTrue(x.length==t1.getLength());
			assertTrue(t1.equals(t3));
			
			assertTrue(t1.getLength() == t2.getLength());
			assertTrue(t1.equals(t2));
			
			Text t4 = new Text(s);
			TextUtils.append(t4,c,c2);
			
			Text t5 = new Text((s+c+c2).getBytes());
			
			assertTrue(t4.getLength() == t5.getLength());
			assertTrue(t4.equals(t5));
			
			
			Text t6 = new Text(s);
			TextUtils.append(t6,s.getBytes(),s2.getBytes(),s3.getBytes());
			
			Text t7 = getLessByteText(s);
			TextUtils.append(t7,s.getBytes(),s2.getBytes(),s3.getBytes());
			
			byte[] xxx = (s+s+s2+s3).getBytes();
			Text t8 = new Text(xxx);
			
			
			assertTrue(xxx.length==t6.getLength());
			assertTrue(t6.equals(t8));
			
			assertTrue(t7.equals(t8));
			
			Text t10 = new Text("真的");
			Text t11 = this.getLessByteText("挺快");
			Text t12 = new Text("的啊");
			TextUtils.append(t10, t11,t12);
			assertTrue(t10.equals(new Text("真的挺快的啊")));
			
		} catch (Exception e) {
			assertTrue(false);
			e.printStackTrace();
		}

	}
	
	@Test
	public void testFind(){
		try{
			byte[] b1 = s1.getBytes();
			byte[] b2 = s2.getBytes();
			assertTrue(TextUtils.find(new Text(s1), s2)==0);
			
			assertTrue(TextUtils.find(new Text(s1), 'a')==b2.length);
			
			assertTrue(TextUtils.find(new Text(s1), 'z')==-1);
			
			assertTrue(TextUtils.find(new Text(), 'a')==-1);
			
			assertTrue(TextUtils.find(this.getLessByteText(s1,"z"), 'z')==-1);
			
			//寻找第N个
			assertTrue(TextUtils.find(this.getLessByteText(s1,s1), 'a',0)==-1);
			assertTrue(TextUtils.find(this.getLessByteText(s1,s1), 'a',1)==b2.length);
			
			Text t = this.getLessByteText(s1+s1,s1);
			assertTrue(TextUtils.find(t, 'a',2)==b2.length+b1.length);

			assertTrue(TextUtils.find(new Text("abcd,,,,,,cbd"), ",,",1)==4);
			assertTrue(TextUtils.find(new Text("abcd,,,,,,cbd"), ",,",2)==6);
			assertTrue(TextUtils.find(new Text("abcd,,,,,,cbd"), ",,",3)==8);
			
			//特殊字符串
			String s1="a,,bcd,e,f,g";
			Text t1 = new Text(s1);
			assertTrue(TextUtils.find(t1,',',2)==2);
			assertTrue(TextUtils.find(t1,',',4)=="a,,bcd,e".length());
			assertTrue(TextUtils.find(t1,',',6)==-1);
			
			//特殊字符串
			String s2=",,,,,";
			Text t2 = new Text(s2);
			assertTrue(TextUtils.find(t2,',',2)==1);
			assertTrue(TextUtils.find(t2,',',4)==3);
			assertTrue(TextUtils.find(t1,',',7)==-1);
			
			
		}
		catch(Exception e){
			assertTrue(false);
			e.printStackTrace();
		}
	}
	
	@Test
	public void testStartsWith(){
		try{
			assertTrue(TextUtils.startsWith(new Text(s1),s1.substring(0,1)));
			assertTrue(TextUtils.startsWith(new Text(s1),s1.substring(0,1).getBytes()));
			assertTrue(TextUtils.startsWith(new Text(s1),c1));
			
			assertTrue(TextUtils.startsWith(this.getLessByteText(s1),s1.substring(0,1)));
			assertTrue(TextUtils.startsWith(this.getLessByteText(s1),s1.substring(0,1).getBytes()));
			assertTrue(TextUtils.startsWith(this.getLessByteText(s1),c1));
			
			assertFalse(TextUtils.startsWith(new Text(s1),s3));
			assertFalse(TextUtils.startsWith(new Text(s1),s3.getBytes()));
			assertFalse(TextUtils.startsWith(new Text(s1),c2));
			
			assertFalse(TextUtils.startsWith(new Text(s3),s1));
			
			assertTrue(TextUtils.startsWith(new Text(s1),s1));
			assertTrue(TextUtils.startsWith(new Text(""),""));
		}
		catch(Exception e){
			assertTrue(false);
		}
	}
	
	@Test
	public void testEndsWith(){
		try{
			assertTrue(TextUtils.endsWith(new Text(s1),"abcd"));
			assertTrue(TextUtils.endsWith(new Text(s1),"abcd".getBytes()));
			assertTrue(TextUtils.endsWith(new Text(s1),"d"));
			
			assertFalse(TextUtils.endsWith(new Text(s1),"真心"));
			assertFalse(TextUtils.endsWith(new Text(s1),"真心".getBytes()));
			assertFalse(TextUtils.endsWith(new Text(s1),"c"));
			
			assertTrue(TextUtils.endsWith(new Text(s1),s1));
			assertTrue(TextUtils.endsWith(new Text(""),""));
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static String ss1="abc,def,ghi,jkl,opq";
	private static String ss2=",abc,def,ghi,jkl,opq,";
	private static String ss3=",,,,,";
	private static String ss4="abcde";
	@Test
	public void testFindField(){
		try{
			Text text = this.getLessByteText(ss1);
			assertTrue(TextUtils.findField(text, "," , 0).equals(new Text("abc")));
			assertTrue(TextUtils.findField(text, "," , 1).equals(new Text("def")));
			assertTrue(TextUtils.findField(text, "," , 4).equals(new Text("opq")));
			
			text = this.getLessByteText(ss2);
			assertTrue(TextUtils.findField(text, "," , 0).equals(new Text("")));
			assertTrue(TextUtils.findField(text, "," , 1).equals(new Text("abc")));
			assertTrue(TextUtils.findField(text, "," , 6).equals(new Text("")));
			
			text = this.getLessByteText(ss3);
			assertTrue(TextUtils.findField(text, "," , 0).equals(new Text("")));
			assertTrue(TextUtils.findField(text, "," , 1).equals(new Text("")));
			assertTrue(TextUtils.findField(text, "," , 5).equals(new Text("")));
			
			text = this.getLessByteText(ss4);
			assertTrue(TextUtils.findField(text, "," , 0).equals(new Text("abcde")));
			
			assertTrue(TextUtils.findField(new Text("abcd,,abc,,,,"), ",," , 1).equals(new Text("abc")));
			assertTrue(TextUtils.findField(new Text("abcd,,abc,,,,"), ",," , 2).equals(new Text("")));
		}
		catch(Exception e){
			assertTrue(false);
		}
	}
	@Test(expected=TextSplitIndexOutOfBoundsException.class)
	public void testFindFieldException1() throws CharacterCodingException{
		Text text = this.getLessByteText(ss1);
		TextUtils.findField(text, "," , 5);
	}
	@Test(expected=TextSplitIndexOutOfBoundsException.class)
	public void testFindFieldException2() throws CharacterCodingException{
		Text text = this.getLessByteText(ss1);
		TextUtils.findField(text, "," , 10);
	}
	@Test(expected=TextSplitIndexOutOfBoundsException.class)
	public void testFindFieldException3() throws CharacterCodingException{
		Text text = this.getLessByteText(ss4);
		assertTrue(TextUtils.findField(text, "," , 1)==null);
	}
	@Test(expected=NullPointerException.class)
	public void testFindFieldException4() throws CharacterCodingException{
		TextUtils.findField(null, "," , 1);
	}
	private boolean compareSpliteResult(Text[] t,String... a){
		if( t.length != a.length ) return false;
		for ( int i=0;i<t.length;i++){
			if( ! t[i].equals(new Text(a[i])) ) return false;
			if( ! t[i].toString().equals(a[i]) ) return false;
		}
		return true;
	}
	@Test
	public void testSplit(){
		try{
			//以下测试split 到2个字段的
			Text text = this.getLessByteText(ss1);
			boolean exceptionError = false;
			try{
				exceptionError = false;
				assertTrue(TextUtils.splitToTwo(text, "," , 0)==null);
			}
			catch(TextSplitIndexOutOfBoundsException e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			
			assertTrue(compareSpliteResult(TextUtils.splitToTwo(text, "," , 1),"abc","def,ghi,jkl,opq"));
			assertTrue(compareSpliteResult(TextUtils.splitToTwo(text, "," , 3),"abc,def,ghi","jkl,opq"));
			
			assertTrue(compareSpliteResult(TextUtils.splitToTwo(new Text("abcd``abcd"), "``" , 1),"abcd","abcd"));
			try{
				exceptionError = false;
				assertTrue(TextUtils.splitToTwo(text, "," , 5)==null);
			}
			catch(TextSplitIndexOutOfBoundsException e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			try{
				exceptionError = false;
				assertTrue(TextUtils.splitToTwo(text, "," , 10)==null);
			}
			catch(TextSplitIndexOutOfBoundsException e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			

			text = this.getLessByteText(ss2);
			
			try{
				exceptionError = false;
				assertTrue(TextUtils.splitToTwo(text, "," , 0)==null);
			}
			catch(TextSplitIndexOutOfBoundsException e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			
			assertTrue(compareSpliteResult(TextUtils.splitToTwo(text, "," , 1),"","abc,def,ghi,jkl,opq,"));
			assertTrue(compareSpliteResult(TextUtils.splitToTwo(text, "," , 6),",abc,def,ghi,jkl,opq",""));

			try{
				exceptionError = false;
				assertTrue(TextUtils.findField(text, "," , 7)==null);
			}
			catch(TextSplitIndexOutOfBoundsException e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			
			text = this.getLessByteText(ss3);
			try{
				exceptionError = false;
				assertTrue(TextUtils.splitToTwo(text, "," , 0)==null);
			}
			catch(TextSplitIndexOutOfBoundsException e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			assertTrue(compareSpliteResult(TextUtils.splitToTwo(text, "," , 1),"",",,,,"));
			assertTrue(compareSpliteResult(TextUtils.splitToTwo(text, "," , 2),",",",,,"));
			assertTrue(compareSpliteResult(TextUtils.splitToTwo(text, "," , 3),",,",",,"));
			assertTrue(compareSpliteResult(TextUtils.splitToTwo(text, "," , 5),",,,,",""));
			
			try{
				exceptionError = false;
				assertTrue(TextUtils.splitToTwo(text, "," , 6)==null);
			}
			catch(TextSplitIndexOutOfBoundsException e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			text = this.getLessByteText(ss4);
			
			try{
				exceptionError = false;
				assertTrue(TextUtils.splitToTwo(text, "," , 0)==null);
			}
			catch(TextSplitIndexOutOfBoundsException e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			try{
				exceptionError = false;
				assertTrue(TextUtils.splitToTwo(text, "," , 1)==null);
			}
			catch(TextSplitIndexOutOfBoundsException e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			//以下测试split所有的
			text = this.getLessByteText(ss1);
			assertTrue(compareSpliteResult(TextUtils.split(text, "," ),"abc","def","ghi","jkl","opq"));
			assertTrue(compareSpliteResult(TextUtils.split(text, "," , 3),"abc","def","ghi,jkl,opq"));
			assertTrue(compareSpliteResult(TextUtils.split(text, "," , 1),"abc,def,ghi,jkl,opq"));
			assertTrue(compareSpliteResult(TextUtils.split(text, "," , 2),"abc","def,ghi,jkl,opq"));
			assertTrue(compareSpliteResult(TextUtils.split(text, "," , 5),"abc","def","ghi","jkl","opq"));
			assertTrue(compareSpliteResult(TextUtils.split(text, "," , 7),"abc","def","ghi","jkl","opq"));
			text = this.getLessByteText(ss2);
			assertTrue(compareSpliteResult(TextUtils.split(text, "," ),"","abc","def","ghi","jkl","opq",""));
			text = this.getLessByteText(ss3);
			assertTrue(compareSpliteResult(TextUtils.split(text, "," ),"","","","","",""));
			text = this.getLessByteText(ss4);
			assertTrue(compareSpliteResult(TextUtils.split(text, "," ),"abcde"));
			assertTrue(compareSpliteResult(TextUtils.split(new Text(), "," ),""));
			assertTrue(compareSpliteResult(TextUtils.split(new Text("abcd,,abc,,ab"), ",," ),"abcd","abc","ab"));
			assertTrue(compareSpliteResult(TextUtils.split(new Text("abcd,,abc,,ab,,"), ",,",3 ),"abcd","abc","ab,,"));
			//以下测试split所有的，且数组需要拓展的情况，即出来的字段大于16个
			text = this.getLessByteText(",0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,,");
			assertTrue(compareSpliteResult(TextUtils.split(text, "," ),"",
					"0","1","2","3","4","5","6","7","8","9",
					"10","11","12","13","14","15","16","17","18","19",
					"20","21","22","23","24","25","26","27","28","29",
					"30","31","32","33","34","35","36","37","38","39","40","",""));
			
		}
		catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void testSubField(){
		try{
			Text text = this.getLessByteText(ss1);
			boolean exceptionError = false;
			try{
				exceptionError = false;
				TextUtils.subField(text,",",4,2);
			}
			catch(Exception e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			assertTrue(TextUtils.subField(text,",",2,4).equals(new Text("ghi,jkl,opq")));
			assertTrue(TextUtils.subField(text,",",2,2).equals(new Text("ghi")));
			assertTrue(TextUtils.subField(text,",",0,0).equals(new Text("abc")));
			assertTrue(TextUtils.subField(text,",",0,4).equals(new Text("abc,def,ghi,jkl,opq")));
			assertTrue(TextUtils.subField(new Text("abcd,,abc,,ab,,a"),",,",1,2).equals(new Text("abc,,ab")));
			try{
				exceptionError = false;
				TextUtils.subField(text,",",0,5);
			}
			catch(Exception e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			text = this.getLessByteText(ss2);
			try{
				exceptionError = false;
				TextUtils.subField(text,",",4,2);
			}
			catch(Exception e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
			
			assertTrue(TextUtils.subField(text,",",2,4).equals(new Text("def,ghi,jkl")));
			assertTrue(TextUtils.subField(text,",",2,2).equals(new Text("def")));
			assertTrue(TextUtils.subField(text,",",0,0).equals(new Text("")));
		}
		catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void testReplaceField(){
		try{
			Text text = this.getLessByteText(ss1);
			boolean exceptionError = false;
			try{
				exceptionError = false;
				TextUtils.splitToTwo(text, "," , 0);
			}
			catch(Exception e){
				exceptionError = true;
			}
			assertTrue(exceptionError);

			Text[] tArray = TextUtils.split(text, ",");
			TextUtils.replaceField(tArray,"abc","def");
			
			assertTrue(compareSpliteResult(tArray,"def","def","ghi","jkl","opq"));
			TextUtils.replaceField(tArray,"def","ghi");
			assertTrue(compareSpliteResult(tArray,"ghi","ghi","ghi","jkl","opq"));
			TextUtils.replaceField(tArray,"ghi","jkl");
			assertTrue(compareSpliteResult(tArray,"jkl","jkl","jkl","jkl","opq"));
			TextUtils.replaceField(tArray,"jkl","");
			assertTrue(compareSpliteResult(tArray,"","","","","opq"));
		}
		catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
			
		}
	}
	@Test
	public void testsubText(){
		try{
			String s="abc,def,ghi,jkl,opq";
			Text text = this.getLessByteText(s);
			assertTrue(TextUtils.subText(text,3).toString().equals(s.substring(3)));
			assertTrue(TextUtils.subText(text,3,5).toString().equals(s.substring(3,5)));
			boolean exceptionError=false;
			try{
				exceptionError = false;
				TextUtils.subText(text,3,50);
			}
			catch(Exception e){
				exceptionError = true;
			}
			assertTrue(exceptionError);
		}
		catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void testJoin(){
		try{
			Text[] text = new Text[]{this.getLessByteText(ss1) , this.getLessByteText(ss2) , this.getLessByteText(ss3)};
			assertTrue(TextUtils.join(text, "`").toString().equals(ss1+"`"+ss2+"`"+ss3));
			assertTrue(TextUtils.join(text, "``").toString().equals(ss1+"``"+ss2+"``"+ss3));
			assertTrue(TextUtils.join(TextUtils.split(this.getLessByteText(ss1), ","),",").toString().equals(ss1));
		}
		catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void testUpperLowerField(){
		Text t = new Text(s1);
		TextUtils.toUpperCase(t);
		t.equals(new Text(s1.toUpperCase()));
		
		TextUtils.toLowerCase(t);
		t.equals(new Text(s1.toLowerCase()));
	}
	
	//below is beachmark
	public void testEncodeBeanchMark(){
		SimpleBenchMark sbm = new SimpleBenchMark(){
			protected String getName() {
				return "encode test";
			}
			protected String test1(int number) {
				for(int i=0;i<number;i++){
					s.getBytes();
				}
				return "String.getBytes()";
			}

			@Override
			protected String test2(int number) {
				for(int i=0;i<number;i++){
					try {
						TextUtils.encode(s);
					} catch (CharacterCodingException e) {
						e.printStackTrace();
					}
				}
				return "TextUtils.encode()";
			}
		};
		sbm.runBeanchMark(1000);
	}
	
	public void testAppendBeanchMark(){
		SimpleBenchMark sbm = new SimpleBenchMark(){
			protected String getName() {
				return "append test";
			}
			protected String test1(int number) {
				for(int i=0;i<number;i++){
					String s = s1+s2+s3;
				}
				return "String + ";
			}
			protected String test2(int number) {
				byte[] b1 = s1.getBytes();
				byte[] b2 = s2.getBytes();
				byte[] b3 = s3.getBytes();
				for(int i=0;i<number;i++){
					Text t = new Text();
					TextUtils.append(t,b1,b2,b3);
				}
				return "TextUtils.append()";
			}
			protected String test3(int number) {
				for(int i=0;i<number;i++){
					StringBuilder sb = new StringBuilder();
					sb.append(s1).append(s2).append(s3);
					String temp =sb.toString();
				}
				return "Stringbuilder  ";
			}
		};
		sbm.runBeanchMark(1000);
	}
	
	public void textSplitAndAppendBeanchMark(){
		SimpleBenchMark sbm = new SimpleBenchMark(){
			protected String getName() {
				return "SplitAndAppend test";
			}
			protected String test1(int number) {
				String temp = "20131022000000`00.03.cd.1a.09.49`1103-4231082793-1121fb48`351101234567890`800`139`8.8.1.243`tangxj@rbbd` `um`ucbrowser`zh-cn` ` `加拿大`android`0` ` ` ` ` `1`1`61.0`1";
				Text t = new Text();
				Text r = new Text();
				for(int i=0;i<number;i++){
					t.set(temp);
					String[] x = t.toString().split("`");
					StringBuilder sb = new StringBuilder();
					for(String s:x){
						sb.append(s);
					}
					sb.toString();
					r.set(sb.toString());
				}
				return "String and stringbuilder ";
			}
			protected String test2(int number) {
				String temp = "20131022000000`00.03.cd.1a.09.49`1103-4231082793-1121fb48`351101234567890`800`139`8.8.1.243`tangxj@rbbd` `um`ucbrowser`zh-cn` ` `加拿大`android`0` ` ` ` ` `1`1`61.0`1";
				byte[] tb = temp.getBytes();
				Text t = new Text(temp);
				Text r = null;
				byte[] split = "`".getBytes();
				for(int i=0;i<number;i++){
					t.set(tb);
					Text[] tArray = TextUtils.split(t, split);
					r = TextUtils.join(tArray, split);
				}
				return "TextUtils.split and append()";
			}
		};
		sbm.runBeanchMark(10000);
	}
	
//	@Test
	public void beanchMark(){
//		testEncodeBeanchMark();
//		testAppendBeanchMark();
		textSplitAndAppendBeanchMark();
	}
	public static void main(String[] arga){
		TestTextUtils ttu = new TestTextUtils();
		ttu.beanchMark();
	}
}
