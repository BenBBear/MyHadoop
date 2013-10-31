package cn.uc.hadoop.utils;

import org.apache.hadoop.io.Text;


public interface TextUtilsInterface {

	//添加一个或者多个对象到Text后，支持str,char,byte[],Text
	public void append(Text text, String... s);
	public void append(Text text, char... c);
	public void append(Text text, byte[]... b);
	public void append(Text text, Text... t);
	
	//检测，寻找字符串
	
	//寻找Text中，第N个目标字符串中，或者，char，或者byte[],返回字节位置
	//注意是返回字节位置，不是char的位置
	public int find(Text text,String str,int n);
	public int find(Text text,char c,int n);
	public int find(Text text,byte[] b,int n);
	//重载N==1的情况
	public int find(Text text,String str);
	public int find(Text text,char c);
	public int find(Text text,byte[] b);
	
	//根据分隔符打断后，获取第N个字段
	//例如text是a,b,c,d 调用findField(text,",",2)后，返回b
	//空字符一样占据字段编号
	//例如text是a,b,,d 调用findField(text,",",4)后，返回d
	public Text findField(Text text,String split,int n);
	public Text findField(Text text,char split,int n);
	public Text findField(Text text,byte[] split,int n);
	
	//根据第N个分隔符打断Text，返回Text数组。
	//例如Text是a,b,c,d 调用split(Text,",",2)后，返回[ Text("a,b")  , Text("c,d") ]
	//例如Text是a,b,c,d 调用split(Text,",",3)后，返回[ Text("a,b,c")  , Text("d") ]
	public Text[] split(Text text,String split,int n);
	public Text[] split(Text text,char split,int n);
	public Text[] split(Text text,byte[] split,int n);
	
	//将Text数组中的，和want相等的替换为pace
	public Text[] replaceField(Text[] text,String want,String place);
	public Text[] replaceField(Text[] text,byte[] want,byte[] place);
	
	//使用Split打断Text，返回从start到end的字段
	public Text subField(Text text,String split,int start,int end);
	public Text subField(Text text,char split,int start,int end);
	public Text subField(Text text,byte[] split,int start,int end);
	
	//转换大小写
	public void toLowerCase(Text text);
	public void toUpperCase(Text text);
	
}
