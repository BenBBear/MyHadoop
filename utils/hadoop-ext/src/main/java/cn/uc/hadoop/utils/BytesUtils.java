package cn.uc.hadoop.utils;

/**
 * 参考了UTF8ByteArrayUtils的代码 实现了一些基本的基于字节数组的比较
 * 
 * @author qiujw
 * 
 */
public class BytesUtils {
	/**
	 * 测试两个数组是否相同
	 * 
	 * @param b1
	 *            待比较数组1
	 * @param s1
	 *            待比较数组1，初始偏移量
	 * @param l1
	 *            待比较数组1，
	 * @param b2
	 *            待比较数组2
	 * @param s2
	 *            偏移量
	 * @param l2
	 *            长度
	 * @return 如果相同则返回true,否则返回false
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
	 * 在原始数组中寻找目标数组b
	 * 
	 * @param utf
	 *            原始字节数组
	 * @param start
	 *            开始下标
	 * @param end
	 *            结束下标
	 * @param b
	 *            目标数组
	 * @return 如果找到目标数组，则返回对应的下标，否则返回-1
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
	 * 在原始数组中,从后往前寻找目标数组b
	 * 
	 * @param utf
	 *            原始字节数组
	 * @param start
	 *            开始下标
	 * @param end
	 *            结束下标
	 * @param b
	 *            目标数组
	 * @return 如果找到目标数组，则返回对应的下标，否则返回-1
	 */
	public static int rfindBytes(byte[] utf, int start, int end, byte[] b) {
		for (int i = end - b.length; i >= 0; i--) {
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
	 * 在原始数组中寻找第N个目标数组b 按照最左匹配原则，寻找到一个目标数组后，将不会对匹配的数组进行重复匹配
	 * 
	 * @param utf
	 *            原始数组
	 * @param start
	 *            开始下标
	 * @param end
	 *            结束下标
	 * @param b
	 *            目标数组
	 * @param n
	 *            寻找第N个目标数组
	 * 
	 * @return 如果找到第N个目标数组，则返回对应的下标，否则返回-1
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
	 * 在原始数组中,从后往前寻找第N个目标数组b 按照最右匹配原则，寻找到一个目标数组后，将不会对匹配的数组进行重复匹配
	 * 
	 * @param utf
	 *            原始数组
	 * @param start
	 *            开始下标
	 * @param end
	 *            结束下标
	 * @param b
	 *            目标数组
	 * @param n
	 *            寻找倒数第N个目标数组
	 * 
	 * @return 如果找到第N个目标数组，则返回对应的下标，否则返回-1
	 */
	public static int rfindNthBytes(byte[] utf, int start, int end, byte[] b,
			int n) {
		int pos = -1;
		int nextEnd = end;
		for (int i = 0; i < n; i++) {
			pos = rfindBytes(utf, start, nextEnd, b);
			if (pos < 0) {
				return pos;
			}
			nextEnd = pos ;
		}
		return pos;
	}
	/**
	 * 计算某个原始数组保护多少个目标数组
	 * @param utf	原始数组
	 * @param start	开始下标
	 * @param end	结束下标
	 * @param b	目标数组
	 * @return	保护的目标的个数
	 */
	public static int countBytes(byte[] utf, int start, int end, byte[] b) {
		int pos = -1;
		int re = 0;
		int nextStart = start;
		while (nextStart < end) {
			pos = findBytes(utf, nextStart, end, b);
			if (pos < 0) {
				return re;
			}
			re++;
			nextStart = pos + b.length;
		}
		return re;
	}

	/**
	 * 测试目标数组是不是原始数组的前缀
	 * 
	 * @param utf
	 *            原始数组
	 * @param start
	 *            开始下标
	 * @param end
	 *            结束下标
	 * @param b
	 *            目标数组
	 * @return 如果，目标数组是测试原始数组的前缀将返回true，否则返回false
	 */

	public static boolean startsWith(byte[] utf, int start, int end, byte[] b) {
		if ((end - start) < b.length) {
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
	 * 测试目标数组是不是原始数组的后缀
	 * 
	 * @param utf
	 *            原始数组
	 * @param start
	 *            开始下标
	 * @param end
	 *            结束下标
	 * @param b
	 *            目标数组
	 * @return 如果，目标数组是测试原始数组的后缀将返回true，否则返回false
	 */
	public static boolean endsWith(byte[] utf, int start, int end, byte[] b) {
		if ((end - start) < b.length) {
			return false;
		}
		for (int i = end - 1, j = b.length - 1; j >= 0; i--, j--) {
			if (utf[i] != b[j]) {
				return false;
			}
		}
		return true;
	}
}
