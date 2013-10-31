package cn.uc.hadoop.utils;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.apache.hadoop.io.Text;

/**
 * 操作Hadoop的Text，关注性能。
 */
public final class TextUtils {
	/**
	 * 线程安全地转换char和string. 如果，转换失败抛出CharacterCodingException
	 */
	private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = new ThreadLocal<CharsetEncoder>() {
		protected CharsetEncoder initialValue() {
			return Charset.forName("UTF-8").newEncoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT);
		}
	};

//	private static ThreadLocal<CharsetDecoder> DECODER_FACTORY = new ThreadLocal<CharsetDecoder>() {
//		protected CharsetDecoder initialValue() {
//			return Charset.forName("UTF-8").newDecoder()
//					.onMalformedInput(CodingErrorAction.REPORT)
//					.onUnmappableCharacter(CodingErrorAction.REPORT);
//		}
//	};

	public static byte[] encode(char c) throws CharacterCodingException {
		return encode(new char[]{c});
	}

	public static byte[] encode(String s) throws CharacterCodingException {
		return encode(s.toCharArray());
	}
	public static byte[] encode(char[] cArray) throws CharacterCodingException {
		ENCODER_FACTORY.get().reset();
		CharBuffer cb = CharBuffer.wrap(cArray);
		ByteBuffer bb =  ENCODER_FACTORY.get().encode(cb);
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
	 * 将c的所有字符中添加到text后面
	 * 
	 * @param text
	 * @param s
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
	 * @param s
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
	 * 将t的Text对象的字节数组都添加到text后面 实现和append(Text text, byte[]... b)类似，但是字节数组的长度有改变
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

	public static int find(Text text, String str)
			throws CharacterCodingException {
		return find(text, str, 1);
	}

	public static int find(Text text, char c) throws CharacterCodingException {
		return find(text, c, 1);
	}

	public static int find(Text text, byte[] b) {
		return find(text, b, 1);
	}

	public static int find(Text text, String str, int n)
			throws CharacterCodingException {
		return find(text, encode(str), n);
	}

	public static int find(Text text, char c, int n)
			throws CharacterCodingException {
		return find(text, encode(c), n);
	}

	public static int find(Text text, byte[] b, int n) {
		return BytesUtils.findNthBytes(text.getBytes(), 0, text.getLength(), b,
				n);
	}

	public static boolean startsWith(Text text, String s)
			throws CharacterCodingException {
		return startsWith(text, encode(s));
	}

	public static boolean startsWith(Text text, char c)
			throws CharacterCodingException {
		return startsWith(text, encode(c));
	}

	public static boolean startsWith(Text text, byte[] b) {
		return BytesUtils.startsWith(text.getBytes(), 0, text.getLength(), b);
	}

	public static boolean endsWith(Text text, String s)
			throws CharacterCodingException {
		return endsWith(text, encode(s));
	}

	public static boolean endsWith(Text text, char c)
			throws CharacterCodingException {
		return endsWith(text, encode(c));
	}

	public static boolean endsWith(Text text, byte[] b) {
		return BytesUtils.endsWith(text.getBytes(), 0, text.getLength(), b);
	}

	// 以下是寻找字段的相关的函数
	// 以下是字段的下标从0开始,
	// 例如 "aa,bb,cc,dd" 分隔符是"," 的情况下有4个字段,aa是下标为0的字段,bb是下标为1的字段
	// 例如 "aabbccdd" 分隔符是"," 的情况下有1个字段,aabbccdd是下标为0的字段
	public static Text findField(Text text, String split, int n)
			throws CharacterCodingException {
		byte[] b = encode(split);
		return findField(text, b, n);
	}

	public static Text findField(Text text, char split, int n)
			throws CharacterCodingException {
		byte[] b = encode(split);
		return findField(text, b, n);
	}

	public static Text findField(Text text, byte[] split, int n) {
		Text re = new Text();
		byte[] b = text.getBytes();
		int end = text.getLength();
		int pos = -1;
		int nextStart = 0;
		int s = -1, e = -1;
		for (int i = 1; i <= n; i++) {
			pos = BytesUtils.findBytes(b, nextStart, end, b);
			if (pos < 0) {
				break;
			} else {
				if (i == n - 1) {
					s = pos;
				} else if (i == n) {
					e = pos;
				}
			}
			nextStart = pos + 1;
		}
		if (s < 0) {
			return re;
		} else {
			if (e < 0) {
				re.set(b, s + 1, (end - s + 1));
				return re;
			} else {
				re.set(b, s + 1, (e - s - 1));
				return re;
			}
		}
	}

	// 以下是Text打断的相关函数,一种是针对一个单一的分隔符，打断为两个Text.一种是类似string的split的全体打断.

	// 例如 "aa,bb,cc,dd" 分隔符是"," 的情况下.按照第2个分隔符打断后，返回["aa,bb" "cc,dd"]
	// 假如目标分隔符不存在，则返回一个包含原来的text的数组
	public Text[] split(Text text, String split, int n)
			throws CharacterCodingException {
		return split(text, encode(split), n);
	}

	public Text[] split(Text text, char split, int n)
			throws CharacterCodingException {
		return split(text, encode(split), n);
	}

	public Text[] split(Text text, byte[] split, int n) {
		byte[] b = text.getBytes();
		int length = text.getLength();
		int pos = BytesUtils.findNthBytes(b, 0, length, split, n);
		if (pos == -1) {
			Text temp = new Text(text);
			return new Text[] { temp };
		} else {
			Text t1 = new Text();
			t1.set(b, 0, pos);
			Text t2 = new Text();
			t2.set(b, pos + 1, (length - pos - 1));
			return new Text[] { t1, t2 };
		}
	}

	// 以下是全体打断函数
	public Text[] split(Text text, String split)
			throws CharacterCodingException {
		return split(text, encode(split));
	}

	public Text[] split(Text text, char split) throws CharacterCodingException {
		return split(text, encode(split));
	}

	public Text[] split(Text text, byte[] split) {
		// TODO 使用静态数组?

		// 采集分割后的下标,如果下标超出maxlength，将复制数组，拓展大小到原来的2倍
		int maxLength = 16;
		int now = 0;
		int[] startMark = new int[maxLength];
		int[] lenMark = new int[maxLength];

		byte[] b = text.getBytes();
		int length = text.getLength();
		int pos = -1;
		int nextStart = 0;
		int pre = 0;
		do {
			pos = BytesUtils.findBytes(b, nextStart, length, b);
			if (pos >= 0) {
				if (now == maxLength) {// 一般情况下都不需要拓展
					int newLength = maxLength << 1;
					int[] temp = new int[newLength];
					System.arraycopy(startMark, 0, temp, 0, maxLength);
					startMark = temp;

					temp = new int[newLength];
					System.arraycopy(lenMark, 0, temp, 0, maxLength);
					lenMark = temp;

					maxLength = newLength;
				}
				startMark[now] = pre;
				lenMark[now] = pos - pre;
				now++;
			}
			nextStart = pos + 1;
		} while (pos >= 0);
		Text[] tArray = new Text[now];
		for (int i = 0; i < now; i++) {
			tArray[i] = new Text();
			if (lenMark[i] != 0) {
				tArray[i].set(b, startMark[i], lenMark[i]);
			}
		}
		return tArray;
	}

	// 以下是Text的字段抠取函数

	public static Text subField(Text text, String split, int start, int end)
			throws CharacterCodingException {
		return subField(text, encode(split), start, end);
	}

	public static Text subField(Text text, char split, int start, int end)
			throws CharacterCodingException {
		return subField(text, encode(split), start, end);
	}

	public static Text subField(Text text, byte[] split, int start, int end) {
		Text re = new Text();
		byte[] b = text.getBytes();
		int length = text.getLength();
		int pos = -1, prepos = -1;
		int nextStart = 0;
		int s = -1, e = -1;
		for (int i = 1; i <= end; i++) {
			pos = BytesUtils.findBytes(b, nextStart, length, b);
			if (pos < 0) {
				if (i == start + 1) {
					s = prepos + 1;
				}
				if (i == end) {
					e = pos - 1;
				}
				break;
			} else {
				if (i == start + 1) {
					s = prepos + 1;
				}
				if (i == end) {
					e = pos - 1;
				}
				prepos = pos;
			}
			nextStart = pos + 1;
		}
		if (s != -1 && e != -1) {
			re.set(b, s, e);
			return re;
		} else {
			return re;
		}

	}

	// 将text数组中进行替换
	public static void replaceField(Text[] text, String want, String place)
			throws CharacterCodingException {
		replaceField(text, encode(want), encode(place));
	}

	public static void replaceField(Text[] text, byte[] want, byte[] place) {
		for (int i = 0; i < text.length; i++) {
			if (BytesUtils.same(text[i].getBytes(), 0, text[i].getLength(),
					want, 0, want.length)) {
				text[i].set(place, 0, place.length);
			}
		}
	}
	
	private static byte upLowDiff = 'A'-'a';
	private static byte aByte = 'a';
	private static byte zByte = 'z';
	private static byte AByte = 'A';
	private static byte ZByte = 'Z';
	public static void toLowerCase(Text text){
		byte[] b = text.getBytes();
		int length = text.getLength();
		for(int i=0;i<length;i++){
			if(b[i]>=AByte && b[i]<= ZByte){
				b[i]-=upLowDiff;
			}
		}
	}
	public static void toUpperCase(Text text){
		byte[] b = text.getBytes();
		int length = text.getLength();
		for(int i=0;i<length;i++){
			if(b[i]>=aByte && b[i]<= zByte){
				b[i]+=upLowDiff;
			}
		}
	}
}
