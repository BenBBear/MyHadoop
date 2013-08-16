package cn.uc.utils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.StringTokenizer;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
//unfinish
class ByteString implements java.io.Serializable, Comparable<String>,
		CharSequence {
	private class StrAtom {
		public StrAtom next;
		public final char c[];
//		public final int offset;
//		public final int count;

		public StrAtom(StrAtom original) {
			this.c = original.c;
//			this.offset = original.offset;
//			this.count = original.count;
			this.next = null;
		}

		public StrAtom(char[] c, int offset, int count) {
			this.c = c;
//			this.offset = offset;
//			this.count = count;
			next = null;
		}

		public void setNext(StrAtom next) {
			this.next = next;
		}

		public StrAtom getNext() {
			return next;
		}
	}

	class CharIterabler {
		private StrAtom now;
		private int nowIndex;
		private int totalIndex;

		CharIterabler() {
			now = null;
			nowIndex = 0;
			totalIndex = 0;
		}

		public void reset() {
			now = head;
			nowIndex = -1;
			totalIndex = -1;
		}

		// skip number char and move the iter to there
		public void skip(int number) {
			if (totalIndex + number < count) {
				totalIndex += number;
				while (nowIndex + number >= now.c.length) {
					// move to next
					now = now.next;
					// 这里余下的步骤数
					number -= ((now.c.length - 1) - nowIndex);
					nowIndex = 0;
				}
				nowIndex += number;
			} else {
				// skip out of total index
			}
		}

		public boolean hashNext() {
			if (now == null) {
				return false;
			}
			if (nowIndex + 1 < now.c.length) {
				return true;
			}
			// as don't allow zero count StrAtom
			// so if next is not null next must have
			if (now.next != null) {
				return true;
			}
			return false;
		}

		public char next() {
			if (nowIndex + 1 < now.c.length) {
				nowIndex++;
			} else {
				now = now.next;
				nowIndex = 0;
			}
			totalIndex++;
			return now.c[nowIndex];
		}

		public int getTotalIndex() {
			return totalIndex;
		}
	}

	private StrAtom head = null;
	private StrAtom tail = null;
	private short atomCount = 0;
	private int count = 0;
	private CharIterabler iter = new CharIterabler();
	/** Cache the hash code for the string */
	private int hash; // Default to 0

	// 链表操作
	private void appendByteAtom(StrAtom byteAtom) {
		if (byteAtom.c.length == 0)
			return;// don't insert zero string
		if (tail == null) {
			head = tail = byteAtom;
			byteAtom.next = null;
		} else {
			tail.next = byteAtom;
			byteAtom.next = null;
		}
		atomCount++;
		count++;
		// hash change , so reset hash to 0
		hash = 0;
	}

	private void init(char[] value, int offset, int count) {
		if (offset < 0) {
			throw new StringIndexOutOfBoundsException(offset);
		}
		if (count < 0) {
			throw new StringIndexOutOfBoundsException(count);
		}
		// Note: offset or count might be near -1>>>1.
		if (offset > value.length - count) {
			throw new StringIndexOutOfBoundsException(offset + count);
		}
		reset();
		appendByteAtom(new StrAtom(value, offset, count));
	}

	// TODO
	private int countHash() {
		return -1;
	}

	private int KMPMatcher(char[] pattern, int prefix[]) {
		int m = pattern.length;
		int q = 0;
		iter.reset();
		int index = 0;
		while (iter.hashNext()) {
			char iChar = iter.next();
			while (q > 0 && pattern[q] != iChar) {
				q = prefix[q - 1];
			}
			if (pattern[q] == iChar)
				q++;
			if (q == m) {
				return index - m;
			}
			index++;
		}
		return -1;
	}

	// search
	private int[] KMPcomputePrefix(char[] pattern) {
		int length = pattern.length;
		int[] prefix = new int[length];
		prefix[0] = 0;
		int k = 0;
		for (int i = 1; i < length; i++) {
			while (k > 0 && pattern[k] != pattern[i]) {
				k = prefix[k - 1];
			}
			if (pattern[k] == pattern[i])
				k++;
			prefix[i] = k;
		}

		return prefix;
	}

	private int indexOf(char[] source, int sourceOffset, int sourceCount,
			char[] target, int targetOffset, int targetCount, int fromIndex) {
		if (fromIndex >= sourceCount) {
			return (targetCount == 0 ? sourceCount : -1);
		}
		if (fromIndex < 0) {
			fromIndex = 0;
		}
		if (targetCount == 0) {
			return fromIndex;
		}

		char first = target[targetOffset];
		int max = sourceOffset + (sourceCount - targetCount);

		for (int i = sourceOffset + fromIndex; i <= max; i++) {
			/* Look for first character. */
			if (source[i] != first) {
				while (++i <= max && source[i] != first)
					;
			}

			/* Found first character, now look at the rest of v2 */
			if (i <= max) {
				int j = i + 1;
				int end = j + targetCount - 1;
				for (int k = targetOffset + 1; j < end
						&& source[j] == target[k]; j++, k++)
					;

				if (j == end) {
					/* Found whole string. */
					return i - sourceOffset;
				}
			}
		}
		return -1;
	}

	// search

	private void compact() {
		if (atomCount > 1) {
			char[] temp = this.getCharA();
			reset();
			init(temp, 0, temp.length);
		}
	}

	// 构造函数结束
	public ByteString() {
		this.atomCount = 0;
	}

	public ByteString(ByteString original) {
		reset();
		// deep copy
		StrAtom temp = original.head;
		while (temp != null) {
			appendByteAtom(new StrAtom(temp));
			temp = temp.next;
		}
	}

	public ByteString(char value[]) {
		this(value, 0, value.length);
	}

	public ByteString(char value[], int offset, int count) {
		init(value, offset, count);
	}

	/*
	 * Common private utility method used to bounds check the byte array and
	 * requested offset & length values used by the String(byte[],..)
	 * constructors.
	 */
	private static void checkBounds(byte[] bytes, int offset, int length) {
		if (length < 0)
			throw new StringIndexOutOfBoundsException(length);
		if (offset < 0)
			throw new StringIndexOutOfBoundsException(offset);
		if (offset > bytes.length - length)
			throw new StringIndexOutOfBoundsException(offset + length);
	}

	public ByteString(byte bytes[], int offset, int length, String charsetName)
			throws UnsupportedEncodingException {
		if (charsetName == null)
			throw new NullPointerException("charsetName");
		checkBounds(bytes, offset, length);
		char[] value = ByteStringCoding.decode(charsetName, bytes, offset,
				length);
		init(value, 0, value.length);
	}

	public ByteString(byte bytes[], int offset, int length, Charset charset) {
		if (charset == null)
			throw new NullPointerException("charset");
		checkBounds(bytes, offset, length);
		char[] value = ByteStringCoding.decode(charset, bytes, offset, length);
		init(value, 0, value.length);
	}

	public ByteString(byte bytes[], String charsetName)
			throws UnsupportedEncodingException {
		this(bytes, 0, bytes.length, charsetName);
	}

	public ByteString(byte bytes[], Charset charset) {
		this(bytes, 0, bytes.length, charset);
	}

	public ByteString(byte bytes[], int offset, int length) {
		checkBounds(bytes, offset, length);
		char[] value = ByteStringCoding.decode(bytes, offset, length);
		init(value, 0, value.length);
	}

	public ByteString(byte bytes[]) {
		this(bytes, 0, bytes.length);
	}

	public ByteString(String s) {
		this(s.toCharArray());
	}

	// 构造函数结束

	public void reset() {
		count = 0;
		atomCount = 0;
		head = tail = null;
		hash = 0;
	}

	public ByteString append(ByteString original) {
		if (original == null)
			return this;
		StrAtom temp = original.head;
		while (temp != null) {
			appendByteAtom(new StrAtom(temp));
			temp = temp.next;
		}
		return this;
	}

	public ByteString[] split(String split) {
		return split(new ByteString(split));
	}

	public ByteString[] split(ByteString split) {
		int pos = indexOf(split);
		if(pos == -1){
			
		}
		return null;
	}

	public int indexOf(ByteString want) {
		return indexOf(want, 0);
	}

	public int indexOf(ByteString want, int fromIndex) {
		compact();
		want.compact();
		return indexOf(this.head.c,0,this.head.c.length,
				want.head.c,0,want.head.c.length,fromIndex);
	}

	public char[] getCharA() {
		iter.reset();
		char[] re = new char[count];
		for (int i = 0; i < count; i++) {
			re[i] = iter.next();
		}
		return re;
	}

	public ByteString substr() {
		// TODO
		return null;
	}

	public int length() {
		return count;
	}

	public boolean isEmpty() {
		return count == 0;
	}

	// common Override
	@Override
	public int hashCode() {
		if (hash == 0) {
			return countHash();
		} else {
			return hash;
		}
	}

	@Override
	public boolean equals(Object anObject) {
		if (this == anObject) {
			return true;
		}
		if (anObject instanceof ByteString) {
			ByteString anotherByteString = (ByteString) anObject;
			int n = count;
			if (n == anotherByteString.count) {
				iter.reset();
				anotherByteString.iter.reset();
				while (iter.hashNext()) {
					if (this.iter.next() != anotherByteString.iter.next()) {
						return false;
					}
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public char charAt(int index) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int compareTo(String o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return new String(getCharA());
	}

	public static void main(String[] args) {
		Random random = new Random();
		final String a = "channel_nbr=997`channel_name=ucweb-app store`type_nbr=uc`publish_nbr=app`area_nbr=international`partner_nbr= `company_nbr=apple`mobile_model= `type_name=uc官方渠道`area_name=国际`company_name=iphone";
		String b = "international";
		String c = "haha";
		testAppend(a, b, c);
		testIndex(a, b);
		testSplit(a,"`");
	}
	public static void testSplit(String a,String b){
		long start, end;
		int times = 100000;
		System.out.println("start " + times + " split");
		System.out.println("string");
		start = System.currentTimeMillis();
		String s = a;
		for (int i = 0; i <= times; i++) {
			s.split(b);
		}
		end = System.currentTimeMillis();
		System.out.println(end - start);
		System.out.println("Bytestring");
		start = System.currentTimeMillis();
		ByteString bs = new ByteString(a);
		ByteString bs_b = new ByteString(b);
		for (int i = 0; i <= times; i++) {
//			bs.split(bs_b);
//			Lists.newArrayList(Splitter.on(b).split(a));
//			Splitter.on(b).split(a);
//			StringTokenizer st = new StringTokenizer(a,b);
//			Lists.newArrayList(st);
			Helper.split(a, b);
		}
		end = System.currentTimeMillis();
		System.out.println(end - start);
	}
	public static void testIndex(String a, String b) {
		long start, end;
		int times = 100000;
		System.out.println("start " + times + " index");
		System.out.println("string");
		start = System.currentTimeMillis();
		String s = a;
		for (int i = 0; i <= times; i++) {
			s.indexOf(b);
		}
		end = System.currentTimeMillis();
		System.out.println(end - start);
		System.out.println("Bytestring");
		start = System.currentTimeMillis();
		ByteString bs = new ByteString(a);
		ByteString bs_b = new ByteString(b);
		for (int i = 0; i <= times; i++) {
			bs.indexOf(bs_b);
		}
		end = System.currentTimeMillis();
		System.out.println(end - start);
	}

	public static void testAppend(String a, String b, String c) {
		long start, end;
		int times = 100000;
		System.out.println("start " + times + " append");
		System.out.println("string");
		start = System.currentTimeMillis();
		String s;
		for (int i = 0; i <= times; i++) {
			s = a;
			s += b;
			s += c;
		}
		end = System.currentTimeMillis();
		System.out.println(end - start);
		System.out.println("Bytestring");
		start = System.currentTimeMillis();
		ByteString bs;
		ByteString bs_b = new ByteString(b);
		ByteString bs_c = new ByteString(c);
		for (int i = 0; i <= times; i++) {
			bs = new ByteString(a);
			bs.append(bs_b).append(bs_c);
		}
		end = System.currentTimeMillis();
		System.out.println(end - start);
	}
}
