package cn.uc.hadoop.utils;

import java.lang.reflect.Array;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

import org.apache.hadoop.io.Text;

/**
 * 操作Hadoop的Text，关注性能。
 */
public final class TextUtils {
	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static byte[] charGetBytes(char ch) {
		CharBuffer cb = CharBuffer.wrap(new char[ch]);
		return UTF8.encode(cb).array();
	}

	public static void append(Text text, char ch) {
		byte[] bs = charGetBytes(ch);
		text.append(bs, 0, bs.length);
	}

	/** 把新的数据附加在Text后面 */
	public static void append(Text text, String str) {
		byte[] bs = str.getBytes(UTF8);
		text.append(bs, 0, bs.length);
	}

	public static boolean endsWith(Text text, String endStr) {
		byte[] eByte = endStr.getBytes(UTF8);
		// 注意长度是length的长度
		byte[] tByte = text.getBytes();
		int tByteLength = text.getLength();
		if (eByte.length > tByteLength)
			return false;
		int i, j;
		for (i = eByte.length - 1, j = tByteLength - 1; i >= 0 && j >= 0; i--, j--) {
			if (eByte[i] != tByte[j])
				return false;
		}
		if (i < 0 || j < 0) {
			return true;
		}
		return false;
	}

	public static int find(Text text, char what) {
		byte[] bs = charGetBytes(what);
		byte[] textByte = text.getBytes();
		int textByteLength = text.getLength();
		if (textByteLength == 0 || bs.length == 0)
			return -1;
		for (int i = 0; i < textByteLength; i++) {
			if (textByte[i] == bs[0])
				return i;
		}
		return -1;
	}

	/** 这个有点难
	 * 支持几个简单的标记 
	 * %d 
	 * %s 
	 */
	public static void format(Text text, String format, Object... params) {
		
	}

	/** 暂不做 */
	public static void replaceAll(Text text, String strA, String strB) {

	}

	public static int rfind(Text text, char ch) {
		return -1;
	}

	public static int rfind(Text text, String what) {
		return -1;
	}

	public static Text[] split(Text text, String splitStr) {
		return null;
	}

	public static boolean startsWith(Text text, String prefix) {
		return text.find(prefix, 0) == 0;
	}

	/**
	 * 
	 * @param text
	 * @param from
	 *            开始位置，以字符为单位
	 * @param len
	 *            长度，以字符为单位
	 * @return
	 */
	public static String subString(Text text, int start, int len) {
		String temp = "";
		temp.format(format, args)
		return null;
	}

	public static void toLowerCase(Text text) {

	}

	public static void toUpperCase(Text text) {

	}
	
	//todo string join with sp
}
