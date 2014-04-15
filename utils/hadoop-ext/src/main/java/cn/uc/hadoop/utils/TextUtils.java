package cn.uc.hadoop.utils;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

import cn.uc.hadoop.exception.TextSplitIndexOutOfBoundsException;

/**
 * <pre>
 * 操作Hadoop的Text，关注性能。
 * 支持包括，字段查找，合并等操作。
 * 每次获取字段，都需要进行一次遍历。如果，频繁操作推荐使用TextRecord。
 * 
 * 什么时候使用TextRecord，什么时候使用TextUtils?
 * 答： TextUtils 是无状态的调用，输出的结果，仅依赖输入。使用比较简单，坑比较少。
 *     但是，如果需要获取多个字段，需要多次遍历这个Text，性能较低。
 *     TextRecord 包括split和内部的Text数组，都是有状态的。
 *     一开始，针对字节数组进行全部打散并保存。
 *     如果，同时处理不同的Text需要创建多个TextRecord。
 *     通过getFeild获取Text，要注意此引用和原来的TextRecord中的Text输关联的。
 *     
 *     推荐：
 *     如果需要对一个记录进行5次以上的的字段获取等操作的时候，使用TextRecord，可获得更好的性能。
 *     否则，使用TextUtils
 * </pre>
 */
public final class TextUtils {
	/**
	 * 线程安全地转换char和string. 如果，转换失败抛出CharacterCodingException
	 */
	private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = new ThreadLocal<CharsetEncoder>() {
		protected CharsetEncoder initialValue() {
			return Charset.forName("UTF-8").newEncoder()
					.onMalformedInput(CodingErrorAction.REPLACE)
					.onUnmappableCharacter(CodingErrorAction.REPLACE);
		}
	};

	// private static ThreadLocal<CharsetDecoder> DECODER_FACTORY = new
	// ThreadLocal<CharsetDecoder>() {
	// protected CharsetDecoder initialValue() {
	// return Charset.forName("UTF-8").newDecoder()
	// .onMalformedInput(CodingErrorAction.REPORT)
	// .onUnmappableCharacter(CodingErrorAction.REPORT);
	// }
	// };

	/**
	 * 将char按照UTF8的编码方式转为byte[]
	 * 
	 * @param c
	 *            要转义的char
	 * @return 对应的byte[]
	 * @throws CharacterCodingException
	 */
	public static byte[] encode(char c) throws CharacterCodingException {
		return encode(new char[] { c });
	}

	/**
	 * 将string按照UTF8的编码方式转为byte[]。跟String.getBytes()类似。
	 * 
	 * @param s
	 *            要转义的String
	 * @return 对应的byte[]
	 * @throws CharacterCodingException
	 */
	public static byte[] encode(String s) throws CharacterCodingException {
		return encode(s.toCharArray());
	}

	/**
	 * 将char[] 按照UTF8的编码方式转为byte[]。
	 * 
	 * @param cArray
	 *            要转义的char[]
	 * @return 对应的byte[]
	 * @throws CharacterCodingException
	 */
	public static byte[] encode(char[] cArray) throws CharacterCodingException {
		ENCODER_FACTORY.get().reset();
		CharBuffer cb = CharBuffer.wrap(cArray);
		ByteBuffer bb = ENCODER_FACTORY.get().encode(cb);
		byte[] temp = new byte[bb.limit()];
		System.arraycopy(bb.array(), 0, temp, 0, bb.limit());
		return temp;
	}

	// 以下是append相关的函数
	/**
	 * 将s的所有字符串中添加到text后面
	 * 
	 * @param text
	 * @param s
	 * @throws CharacterCodingException
	 */
	public static void append(Text text, String... s)
			throws CharacterCodingException {
		int length = s.length;
		byte[][] bArray = new byte[s.length][];
		for (int i = 0; i < length; i++) {
			bArray[i] = encode(s[i]);
		}
		append(text, bArray);
	}

	/**
	 * 将c的所有字符添加到text后面
	 * 
	 * @param text
	 * @param c
	 * @throws CharacterCodingException
	 */
	public static void append(Text text, char... c)
			throws CharacterCodingException {
		int length = c.length;
		byte[][] bArray = new byte[c.length][];
		for (int i = 0; i < length; i++) {
			bArray[i] = encode(c[i]);
		}
		append(text, bArray);
	}

	/**
	 * 将b的所有字节数组添加到text后面
	 * 
	 * @param text
	 * @param b
	 * @throws CharacterCodingException
	 */
	public static void append(Text text, byte[]... b) {
		int length = b.length;
		int sumLength = text.getLength();
		for (int i = 0; i < length; i++) {
			sumLength += b[i].length;
		}
		byte[] dest = new byte[sumLength];
		int destPos = 0;
		// 复制原来的text
		System.arraycopy(text.getBytes(), 0, dest, destPos, text.getLength());
		destPos += text.getLength();
		// 添加新增的text
		for (int i = 0; i < length; i++) {
			System.arraycopy(b[i], 0, dest, destPos, b[i].length);
			destPos += b[i].length;
		}
		text.set(dest);
	}

	/**
	 * 将t的Text对象的字节数组都添加到text后面
	 * 
	 * @param text
	 * @param t
	 * @throws CharacterCodingException
	 */
	public static void append(Text text, Text... t) {
		int length = t.length;
		int sumLength = text.getLength();
		for (int i = 0; i < length; i++) {
			sumLength += t[i].getLength();
		}
		byte[] dest = new byte[sumLength];
		int destPos = 0;
		// 复制原来的text
		System.arraycopy(text.getBytes(), 0, dest, destPos, text.getLength());
		destPos += text.getLength();
		// 添加新增的text
		for (int i = 0; i < length; i++) {
			System.arraycopy(t[i].getBytes(), 0, dest, destPos,
					t[i].getLength());
			destPos += t[i].getLength();
		}
		text.set(dest);
	}

	// 以下是寻找字符串的相关的函数
	/**
	 * 在Text中寻找目标字符串是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param str
	 *            目标字符串
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int find(Text text, String str)
			throws CharacterCodingException {
		return find(text, str, 1);
	}
	
	/**
	 * 在Text中从后往前寻找目标字符串是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param str
	 *            目标字符串
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int rfind(Text text, String str)
			throws CharacterCodingException {
		return rfind(text, str, 1);
	}
	

	/**
	 * 在Text中寻找目标字符是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param c
	 *            目标字符
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int find(Text text, char c) throws CharacterCodingException {
		return find(text, c, 1);
	}
	
	/**
	 * 在Text中从后往前寻找目标字符是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param c
	 *            目标字符
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int rfind(Text text, char c) throws CharacterCodingException {
		return rfind(text, c, 1);
	}

	/**
	 * 在Text中寻找目标字节数组是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param b
	 *            目标字节数组
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int find(Text text, byte[] b) {
		return find(text, b, 1);
	}
	/**
	 * 在Text中从后向前寻找目标字节数组是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param b
	 *            目标字节数组
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int rfind(Text text, byte[] b) {
		return rfind(text, b, 1);
	}

	/**
	 * 在Text中寻找第N个目标字符串是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param str
	 *            目标字符串
	 * @param n
	 *            第N个目标字符串
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int find(Text text, String str, int n)
			throws CharacterCodingException {
		return find(text, encode(str), n);
	}
	
	/**
	 * 在Text中,从后向前寻找第N个目标字符串是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param str
	 *            目标字符串
	 * @param n
	 *            第N个目标字符串
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int rfind(Text text, String str, int n)
			throws CharacterCodingException {
		return rfind(text, encode(str), n);
	}
	
	/**
	 * 在Text中寻找第N个目标字符是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param c
	 *            目标字符
	 * @param n
	 *            第N个目标字符
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int find(Text text, char c, int n)
			throws CharacterCodingException {
		return find(text, encode(c), n);
	}
	
	/**
	 * 在Text中从后向前寻找第N个目标字符是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param c
	 *            目标字符
	 * @param n
	 *            第N个目标字符
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int rfind(Text text, char c, int n)
			throws CharacterCodingException {
		return rfind(text, encode(c), n);
	}
	
	/**
	 * 在Text中寻找第N个目标字节数组是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param b
	 *            目标字节数组
	 * @param n
	 *            第N个目标字节数组
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int find(Text text, byte[] b, int n) {
		return BytesUtils.findNthBytes(text.getBytes(), 0, text.getLength(), b,
				n);
	}
	
	/**
	 * 在Text中从后往前寻找第N个目标字节数组是否存在，返回字节下标
	 * 
	 * @param text
	 *            原始Text
	 * @param b
	 *            目标字节数组
	 * @param n
	 *            第N个目标字节数组
	 * @return 如果找到返回,字节下标，否则返回-1
	 * @throws CharacterCodingException
	 */
	public static int rfind(Text text, byte[] b, int n) {
		return BytesUtils.rfindNthBytes(text.getBytes(), 0, text.getLength(), b,
				n);
	}
	
	/**
	 * 测试Text的前缀是不是目标字符串
	 * 
	 * @param text
	 *            原始Text
	 * @param s
	 *            目标字符串
	 * @return 如果是，返回true，否则返回false
	 * @throws CharacterCodingException
	 */
	public static boolean startsWith(Text text, String s)
			throws CharacterCodingException {
		return startsWith(text, encode(s));
	}

	/**
	 * 测试Text的前缀是不是目标字符
	 * 
	 * @param text
	 *            原始Text
	 * @param c
	 *            目标字符
	 * @return 如果是，返回true，否则返回false
	 * @throws CharacterCodingException
	 */
	public static boolean startsWith(Text text, char c)
			throws CharacterCodingException {
		return startsWith(text, encode(c));
	}

	/**
	 * 测试Text的前缀是不是目标字节数组
	 * 
	 * @param text
	 *            原始Text
	 * @param b
	 *            目标字节数组
	 * @return 如果是，返回true，否则返回false
	 * @throws CharacterCodingException
	 */
	public static boolean startsWith(Text text, byte[] b) {
		return BytesUtils.startsWith(text.getBytes(), 0, text.getLength(), b);
	}

	/**
	 * 测试Text的后缀是不是目标字符串
	 * 
	 * @param text
	 *            原始Text
	 * @param s
	 *            目标字符串
	 * @return 如果是，返回true，否则返回false
	 * @throws CharacterCodingException
	 */
	public static boolean endsWith(Text text, String s)
			throws CharacterCodingException {
		return endsWith(text, encode(s));
	}

	/**
	 * 测试Text的后缀是不是目标字符
	 * 
	 * @param text
	 *            原始Text
	 * @param c
	 *            目标字符
	 * @return 如果是，返回true，否则返回false
	 * @throws CharacterCodingException
	 */
	public static boolean endsWith(Text text, char c)
			throws CharacterCodingException {
		return endsWith(text, encode(c));
	}

	/**
	 * 测试Text的后缀是不是目标字节数组
	 * 
	 * @param text
	 *            原始Text
	 * @param b
	 *            目标字节数组
	 * @return 如果是，返回true，否则返回false
	 * @throws CharacterCodingException
	 */
	public static boolean endsWith(Text text, byte[] b) {
		return BytesUtils.endsWith(text.getBytes(), 0, text.getLength(), b);
	}

	/**
	 * Text的截取操作类似String的substring操作
	 * 
	 * @param text
	 *            原始Text
	 * @param start
	 *            截取的开始偏移量
	 * @param end
	 *            截取的结束偏移量
	 * @return
	 */
	public static Text subText(Text text, int start) {
		return subText(text, start, text.getLength());
	}

	/**
	 * Text的截取操作类似String的substring操作
	 * 
	 * @param text
	 *            原始Text
	 * @param start
	 *            截取的开始偏移量
	 * @param end
	 *            截取的结束偏移量
	 * @return
	 */
	public static Text subText(Text text, int start, int end) {
		if (end > text.getLength()) {
			throw new TextSplitIndexOutOfBoundsException(end);
		}
		if (start < 0 || start >= text.getLength()) {
			throw new TextSplitIndexOutOfBoundsException(start);
		}
		if (start >= end) {
			throw new TextSplitIndexOutOfBoundsException(end - start);
		}
		byte[] b = text.getBytes();
		Text t = new Text();
		t.set(b, start, (end - start));
		return t;
	}

	// 以下是寻找字段的相关的函数
	// 以下是字段的下标从0开始,
	// 例如 "aa,bb,cc,dd" 分隔符是"," 的情况下有4个字段,aa是下标为0的字段,bb是下标为1的字段
	// 例如 "aabbccdd" 分隔符是"," 的情况下有1个字段,aabbccdd是下标为0的字段
	// 例如 "aabbccdd" 获取第二个字段将会返回null
	// 例如 "aabb,,ccdd" 获取第二个字段将会返回"" (非空，长度为0的字符串)
	/**
	 * 根据分隔符将Text分割后，寻找第N个字段，字段下标从0开始
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param n
	 *            第n个字段
	 * @return 返回第n个字段
	 * @throws CharacterCodingException
	 */
	public static Text findField(Text text, String split, int n)
			throws CharacterCodingException {
		byte[] b = encode(split);
		return findField(text, b, n);
	}
	
	/**
	 * 根据分隔符将Text分割后，从后往前寻找第N个字段，字段下标从0开始
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param n
	 *            第n个字段
	 * @return 返回第n个字段
	 * @throws CharacterCodingException
	 */
	public static Text rfindField(Text text, String split, int n)
			throws CharacterCodingException {
		byte[] b = encode(split);
		return rfindField(text, b, n);
	}
	
	/**
	 * 根据分隔符将Text分割后，寻找第N个字段，字段下标从0开始
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param n
	 *            第n个字段
	 * @return 返回第n个字段
	 * @throws CharacterCodingException
	 */
	public static Text findField(Text text, char split, int n)
			throws CharacterCodingException {
		byte[] b = encode(split);
		return findField(text, b, n);
	}
	
	/**
	 * 根据分隔符将Text分割后，从后往前寻找第N个字段，字段下标从0开始
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param n
	 *            第n个字段
	 * @return 返回第n个字段
	 * @throws CharacterCodingException
	 */
	public static Text rfindField(Text text, char split, int n)
			throws CharacterCodingException {
		byte[] b = encode(split);
		return rfindField(text, b, n);
	}

	/**
	 * 根据分隔符将Text分割后，寻找第N个字段，字段下标从0开始
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param n
	 *            第n个字段
	 * @return 返回第n个字段
	 * @throws CharacterCodingException
	 */
	public static Text findField(Text text, byte[] split, int n) {
		byte[] b = text.getBytes();
		int end = text.getLength();
		int pos = -1;
		int nextStart = 0;
		int s = -1, e = -1;
		int i = 0;
		for (i = 0; i <= n; i++) {
			pos = BytesUtils.findBytes(b, nextStart, end, split);
			if (pos < 0) {
				break;
			} else {
				if (i == n) {
					s = nextStart;
					e = pos;
					break;
				}
			}
			nextStart = pos + split.length;
		}
		// 寻找到最后一个
		if (pos < 0 && i == n) {
			s = nextStart;
			e = end;
		}
		if (s < 0) {
			throw new TextSplitIndexOutOfBoundsException(n);
		} else {
			Text re = new Text();
			re.set(b, s, e - s);
			return re;
		}
	}
	/**
	 * 根据分隔符将Text分割后，从后往前寻找第N个字段，字段下标从0开始
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param n
	 *            第n个字段
	 * @return 返回第n个字段
	 * @throws CharacterCodingException
	 */
	public static Text rfindField(Text text, byte[] split, int n) {
		byte[] b = text.getBytes();
		int end = text.getLength();
		int pos = -1;
		int nextEnd = end;
		int s = -1, e = -1;
		int i = 0;
		for (i = 0; i <= n; i++) {
			pos = BytesUtils.rfindBytes(b, 0, nextEnd, split);
			if (pos < 0) {
				break;
			} else {
				if (i == n) {
					s = pos + split.length;
					e = nextEnd;
					break;
				}
			}
			nextEnd = pos ;
		}
		// 寻找到最后一个
		if (pos < 0 && i == n) {
			s = 0;
			e = nextEnd;
		}
		if (s < 0) {
			throw new TextSplitIndexOutOfBoundsException(n);
		} else {
			Text re = new Text();
			re.set(b, s, e - s);
			return re;
		}
	}

	// 以下是Text打断的相关函数,一种是针对一个单一的分隔符，打断为两个Text.一种是类似string的split的全体打断.

	// 例如 "aa,bb,cc,dd" 分隔符是"," 的情况下.按照第2个分隔符打断后，返回["aa,bb" "cc,dd"]
	// 假如目标分隔符不存在，则返回null
	/**
	 * 根据第N个分隔符将Text打断为两个Text 例如 "aa,bb,cc,dd" 分隔符是","
	 * 的情况下.按照第2个分隔符打断后，返回["aa,bb" "cc,dd"]
	 * 如果N为负数,将从后面开始找起。
	 * 
	 * @param text
	 *            原始的Text
	 * @param split
	 *            分隔符
	 * @param n
	 *            第n个分隔符（从1开始),如果为负数,将从后面开始找起
	 * @return 分割后的两个Text
	 * @throws CharacterCodingException
	 */
	public static Text[] splitToTwo(Text text, String split, int n)
			throws CharacterCodingException {
		return splitToTwo(text, encode(split), n);
	}

	/**
	 * 根据第N个分隔符将Text打断为两个Text 例如 "aa,bb,cc,dd" 分隔符是","
	 * 的情况下.按照第2个分隔符打断后，返回["aa,bb" "cc,dd"]
	 * 
	 * @param text
	 *            原始的Text
	 * @param split
	 *            分隔符
	 * @param n
	 *            第n个分隔符（从1开始)
	 * @return 分割后的两个Text
	 * @throws CharacterCodingException
	 */
	public static Text[] splitToTwo(Text text, char split, int n)
			throws CharacterCodingException {
		return splitToTwo(text, encode(split), n);
	}

	/**
	 * 根据第N个分隔符将Text打断为两个Text 例如 "aa,bb,cc,dd" 分隔符是","
	 * 的情况下.按照第2个分隔符打断后，返回["aa,bb" "cc,dd"]
	 * 
	 * @param text
	 *            原始的Text
	 * @param split
	 *            分隔符
	 * @param n
	 *            第n个分隔符（从1开始)
	 * @return 分割后的两个Text
	 * @throws CharacterCodingException
	 */
	public static Text[] splitToTwo(Text text, byte[] split, int n) {
		if (text == null)
			return null;
		byte[] b = text.getBytes();
		int length = text.getLength();
		int pos = -1;
		if( n > 0 ){
			pos = BytesUtils.findNthBytes(b, 0, length, split, n);
		}
		else{
			pos = BytesUtils.rfindNthBytes(b, 0, length, split, -n);
		}
		if (pos == -1) {
			throw new TextSplitIndexOutOfBoundsException(n);
		} else {
			Text t1 = new Text();
			t1.set(b, 0, pos);
			Text t2 = new Text();
			t2.set(b, pos + split.length, (length - pos - split.length));
			return new Text[] { t1, t2 };
		}
	}

	// 以下是全体打断函数
	/**
	 * 根据分隔符将Text全部打断
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @return 打断后的Text数组
	 * @throws CharacterCodingException
	 */
	public static Text[] split(Text text, String split)
			throws CharacterCodingException {
		return split(text, encode(split), 0);
	}

	/**
	 * 根据分隔符将Text全部打断
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @return 打断后的Text数组
	 * @throws CharacterCodingException
	 */
	public static Text[] split(Text text, char split)
			throws CharacterCodingException {
		return split(text, encode(split), 0);
	}

	/**
	 * 根据分隔符将Text全部打断
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @return 打断后的Text数组
	 * @throws CharacterCodingException
	 */
	public static Text[] split(Text text, byte[] split) {
		return split(text, split, 0);
	}

	/**
	 * 根据分隔符将Text全部打断，最多分割为limit份
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param limit
	 *            最多分割份数
	 * @return 打断后的Text数组
	 * @throws CharacterCodingException
	 */
	public static Text[] split(Text text, String split, int limit)
			throws CharacterCodingException {
		return split(text, encode(split), limit);
	}

	/**
	 * 根据分隔符将Text全部打断，最多分割为limit份
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param limit
	 *            最多分割份数
	 * @return 打断后的Text数组
	 * @throws CharacterCodingException
	 */
	public static Text[] split(Text text, char split, int limit)
			throws CharacterCodingException {
		return split(text, encode(split), limit);
	}

	/**
	 * 根据分隔符将Text全部打断，最多分割为limit份
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param limit
	 *            最多分割份数
	 * @return 打断后的Text数组
	 * @throws CharacterCodingException
	 */
	public static Text[] split(Text text, byte[] split, int limit) {
		if (limit == 1) {
			return new Text[] { new Text(text) };
		}
		// TODO 使用静态数组?
		// 采集分割后的下标,如果下标超出maxlength，将复制数组，拓展大小到原来的2倍
		int maxLength = 16;
		if (limit > 1) {
			// 如果固定了切分的数量,则最大的标记数组时可以预计的
			maxLength = limit + 1;
		}
		int now = 0;
		int[] startMark = new int[maxLength];
		int[] endMark = new int[maxLength];

		byte[] b = text.getBytes();
		int length = text.getLength();
		int pos = -1;
		int nextStart = 0;
		do {
			pos = BytesUtils.findBytes(b, nextStart, length, split);
			if (now == maxLength) {// 一般情况下都不需要拓展
				int newLength = maxLength << 1;
				int[] temp = new int[newLength];
				System.arraycopy(startMark, 0, temp, 0, maxLength);
				startMark = temp;

				temp = new int[newLength];
				System.arraycopy(endMark, 0, temp, 0, maxLength);
				endMark = temp;

				maxLength = newLength;
			}
			if (pos >= 0) {
				startMark[now] = nextStart;
				endMark[now] = pos;
				now++;
				// 到达上限了
				if (now == limit - 1) {
					startMark[now] = pos + split.length;
					endMark[now] = length;
					now++;
					break;
				}
			} else {
				startMark[now] = nextStart;
				endMark[now] = length;
				now++;
			}
			nextStart = pos + split.length;
		} while (pos >= 0);
		// 复制字节到数组中
		Text[] tArray = new Text[now];
		for (int i = 0; i < now; i++) {
			tArray[i] = new Text();
			if (endMark[i] != 0) {
				tArray[i].set(b, startMark[i], (endMark[i] - startMark[i]));
			}
		}
		return tArray;
	}

	// 以下是Text的字段抠取函数
	/**
	 * Text的字段抠取函数。 根据分隔符打断Text后，获取从第start字段到第end字段的Text。 例如:原始Text是 a,b,c,d,e
	 * 获取第2 到3字段的text是 c,d
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param start
	 *            开始的字段下标
	 * @param end
	 *            结束的字段下标
	 * @return 抠取的Text
	 * @throws CharacterCodingException
	 */
	public static Text subField(Text text, String split, int start, int end)
			throws CharacterCodingException {
		return subField(text, encode(split), start, end);
	}

	/**
	 * Text的字段抠取函数。 根据分隔符打断Text后，获取从第start字段到第end字段的Text。 例如:原始Text是 a,b,c,d,e
	 * 获取第2 到3字段的text是 c,d
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param start
	 *            开始的字段下标
	 * @param end
	 *            结束的字段下标
	 * @return 抠取的Text
	 * @throws CharacterCodingException
	 */
	public static Text subField(Text text, char split, int start, int end)
			throws CharacterCodingException {
		return subField(text, encode(split), start, end);
	}

	/**
	 * Text的字段抠取函数。 根据分隔符打断Text后，获取从第start字段到第end字段的Text。 例如:原始Text是 a,b,c,d,e
	 * 获取第2 到3字段的text是 c,d
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @param start
	 *            开始的字段下标
	 * @param end
	 *            结束的字段下标
	 * @return 抠取的Text
	 * @throws CharacterCodingException
	 */
	public static Text subField(Text text, byte[] split, int start, int end) {
		if (start < 0) {
			throw new TextSplitIndexOutOfBoundsException(start);
		}
		if (end < 0) {
			throw new TextSplitIndexOutOfBoundsException(end);
		}
		if (start > end) {
			throw new TextSplitIndexOutOfBoundsException(end - start);
		}

		byte[] b = text.getBytes();
		int length = text.getLength();
		int pos = -1;
		int nextStart = 0;
		int s = -1, e = -1, i;
		for (i = 0; i <= end; i++) {
			pos = BytesUtils.findBytes(b, nextStart, length, split);
			if (pos < 0) {
				break;
			} else {
				if (i == start) {
					s = nextStart;
				}
				if (i == end) {
					e = pos;
				}
			}
			nextStart = pos + split.length;
		}
		if (i == start && pos < 0) {
			e = nextStart;
		}
		if (i == end && pos < 0) {
			e = length;
		}
		if (s == -1) {
			throw new TextSplitIndexOutOfBoundsException(start);
		}
		if (e == -1) {
			throw new TextSplitIndexOutOfBoundsException(end);
		}
		Text re = new Text();
		re.set(b, s, e - s);
		return re;
	}

	// 将text数组中进行替换
	/**
	 * 将原始Text中存在的字符串替换为另一个字符串
	 * 
	 * @param text
	 *            原始Text
	 * @param want
	 *            要寻找的字符串
	 * @param place
	 *            替换后的字符串
	 * @throws CharacterCodingException
	 */
	public static void replaceField(Text[] text, String want, String place)
			throws CharacterCodingException {
		replaceField(text, encode(want), encode(place));
	}

	/**
	 * 将原始Text中存在的字节数组替换为另一个字节数组
	 * 
	 * @param text
	 *            原始Text
	 * @param want
	 *            要寻找的字节数组
	 * @param place
	 *            替换后的字节数组
	 */
	public static void replaceField(Text[] text, byte[] want, byte[] place) {
		for (int i = 0; i < text.length; i++) {
			if (BytesUtils.same(text[i].getBytes(), 0, text[i].getLength(),
					want, 0, want.length)) {
				text[i].set(place, 0, place.length);
			}
		}
	}

	// 将text数组使用指定的分隔符进行拼接,合并到一个text中
	/**
	 * 使用指定的分隔符将Text数组进行拼接输出
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @return 拼接后的Text
	 * @throws CharacterCodingException
	 */
	public static Text join(Text[] text, String split)
			throws CharacterCodingException {
		return join(text, encode(split));
	}

	/**
	 * 使用指定的分隔符将Text数组进行拼接输出
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @return 拼接后的Text
	 * @throws CharacterCodingException
	 */
	public static Text join(Text[] text, char split)
			throws CharacterCodingException {
		return join(text, encode(split));
	}

	/**
	 * 使用指定的分隔符将Text数组进行拼接输出
	 * 
	 * @param text
	 *            原始Text
	 * @param split
	 *            分隔符
	 * @return 拼接后的Text
	 * @throws CharacterCodingException
	 */
	public static Text join(Text[] text, byte[] split) {
		return join(text, text.length, split);
	}

	/**
	 * 使用指定的分隔符将Text数组的前n个Text进行拼接输出
	 * 
	 * @param text
	 *            原始Text数组
	 * @param n
	 *            Text数组的前N个Text
	 * @param split
	 *            分隔符
	 * @return 拼接后的Text
	 * @throws CharacterCodingException
	 */
	public static Text join(Text[] text, int n, byte[] split) {
		if (n == 0) {
			return new Text();
		}
		int sumLength = 0;
		for (int i = 0; i < n; i++) {
			sumLength += text[i].getLength();
			sumLength += split.length;
		}
		// 减去最后一个
		sumLength -= split.length;

		byte[] dest = new byte[sumLength];

		int destPos = 0;
		// 复制原来的text
		destPos = 0;
		// 添加新增的text
		for (int i = 0; i < n; i++) {
			if (i != 0) {
				// 复制一个split
				System.arraycopy(split, 0, dest, destPos, split.length);
				destPos += split.length;
			}
			System.arraycopy(text[i].getBytes(), 0, dest, destPos,
					text[i].getLength());
			destPos += text[i].getLength();
		}
		Text re = new Text();
		re.set(dest);
		return re;
	}

	private static byte upLowDiff = 'A' - 'a';
	private static byte aByte = 'a';
	private static byte zByte = 'z';
	private static byte AByte = 'A';
	private static byte ZByte = 'Z';

	/**
	 * 将Text的大写转为小写
	 * 
	 * @param text
	 *            要转换的Text
	 */
	public static void toLowerCase(Text text) {
		byte[] b = text.getBytes();
		int length = text.getLength();
		for (int i = 0; i < length; i++) {
			if (b[i] >= AByte && b[i] <= ZByte) {
				b[i] -= upLowDiff;
			}
		}
	}

	/**
	 * 将Text的小写转为大写
	 * 
	 * @param text
	 *            要转换的Text
	 */
	public static void toUpperCase(Text text) {
		byte[] b = text.getBytes();
		int length = text.getLength();
		for (int i = 0; i < length; i++) {
			if (b[i] >= aByte && b[i] <= zByte) {
				b[i] += upLowDiff;
			}
		}
	}
	/**
	 * 将一个Text按照字段分隔符和keyvalue分隔符进行切分。
	 * 例如: a=b`c=d 切分后返回 ((a,b),(c,d))的映射
	 * @param text 要切分的Text
	 * @param fieldSplit 字段分隔符
	 * @param kvSplit keyvalue分隔符
	 * @return 返回一个保护多个KV对的映射
	 */
	public static Map<Text, Text> splitToKV(Text text, byte[] fieldSplit,
			byte[] kvSplit) {
		// 是否启动预采集
		// 根据测试 hashmap性能最好，默认的容量，也OK
		Map<Text, Text>	map = new HashMap<Text, Text>();
		byte[] b = text.getBytes();
		int length = text.getLength();
		int pos = -1;
		int nextStart = 0;
		int begin=0,end=0;
		do {
			pos = BytesUtils.findBytes(b, nextStart, length, fieldSplit);
			if (pos >= 0) {
				// nextStart pos
				begin = nextStart;
				end = pos;
			} else {
				// nextStart length
				begin = nextStart;
				end = length;
			}
			{
				int kvMid = BytesUtils.findBytes(b, begin, end, kvSplit);
				if (kvMid != -1) {
					Text k = new Text();
					k.set(b, begin, (kvMid - begin));
					Text v = new Text();
					v.set(b, kvMid + kvSplit.length, end - (kvMid + kvSplit.length));
					map.put(k, v);
				}
			}
			nextStart = pos + fieldSplit.length;
		} while (pos >= 0);
		return map;
	}
}
