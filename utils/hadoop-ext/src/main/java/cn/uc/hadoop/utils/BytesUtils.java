package cn.uc.hadoop.utils;

/**
 * 参考了UTF8ByteArrayUtils的代码
 * 
 * @author qiujw
 * 
 */
public class BytesUtils {
	/**
	 * Tests weather the b1 and b2 is the same
	 * 
	 * @param b1
	 *            byte array want to compare
	 * @param s1
	 *            starting offset
	 * @param l1
	 *            byte's length
	 * @param b2
	 *            byte array want to compare
	 * @param s2
	 *            starting offset
	 * @param l2
	 *            byte's length
	 * @return return true ,if b1 is the same with b2.Return false or not.
	 */

	public static boolean same(byte[] b1, int s1, int l1, byte[] b2, int s2,
			int l2) {
		if (l1 != l2) {
			return false;
		}
		for (int i = 0; i < l1; i++) {
			if (b1[s1 + i] != b2[s2 + i]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Find the first occurrence of the given bytes b in a UTF-8 encoded string
	 * 
	 * @param utf
	 *            a byte array containing a UTF-8 encoded string
	 * @param start
	 *            starting offset
	 * @param end
	 *            ending position
	 * @param b
	 *            the bytes to find
	 * @return position that first byte occures otherwise -1
	 */
	public static int findBytes(byte[] utf, int start, int end, byte[] b) {
		int matchEnd = end - b.length;
		for (int i = start; i <= matchEnd; i++) {
			boolean matched = true;
			for (int j = 0; j < b.length; j++) {
				if (utf[i + j] != b[j]) {
					matched = false;
					break;
				}
			}
			if (matched) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Find the nth occurrence of the given byte b in a UTF-8 encoded string
	 * 
	 * @param utf
	 *            a byte array containing a UTF-8 encoded string
	 * @param start
	 *            starting offset
	 * @param end
	 *            ending position
	 * @param b
	 *            the byte to find
	 * @param n
	 *            the desired occurrence of the given byte
	 * @return position that nth occurrence of the given byte if exists;
	 *         otherwise -1
	 */
	public static int findNthBytes(byte[] utf, int start, int end, byte[] b,
			int n) {
		int pos = -1;
		int nextStart = start;
		for (int i = 0; i < n; i++) {
			pos = findBytes(utf, nextStart, end, b);
			if (pos < 0) {
				return pos;
			}
			nextStart = pos + b.length;
		}
		return pos;
	}

	/**
	 * Tests if this byte starts with the specified byte.
	 * 
	 * @param utf
	 *            a byte array containing a UTF-8 encoded string
	 * @param start
	 *            starting offset
	 * @param end
	 *            ending position
	 * @param b
	 *            the byte to find
	 * @return true if the byte starts with the specified byte.False or not.
	 */

	public static boolean startsWith(byte[] utf, int start, int end, byte[] b) {
		if ( (end - start) < b.length) {
			return false;
		}
		for (int i = start, j = 0; j < b.length; i++, j++) {
			if (utf[i] != b[j]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Tests if this byte ends with the specified byte.
	 * 
	 * @param utf
	 *            a byte array containing a UTF-8 encoded string
	 * @param start
	 *            starting offset
	 * @param end
	 *            ending position
	 * @param b
	 *            the byte to find
	 * @return true if the byte starts with the specified byte.False or not.
	 */

	public static boolean endsWith(byte[] utf, int start, int end, byte[] b) {
		if( (end - start) < b.length) {
			return false;
		}
		for (int i = end-1 , j = b.length-1; j >=0; i--, j--) {
			if (utf[i] != b[j]) {
				return false;
			}
		}
		return true;
	}
}
