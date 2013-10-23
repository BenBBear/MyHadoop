package cn.uc.hadoop.utils;

/**
 * <pre>
 * 16进制字符串，模仿二进制位测试。字符仅能是16进制，忽略大小写，如果不是16进制字符，按0计算。
 * 如果指定位置上没有设置值，按0计算（这个特性可以用于忽略前导0）。
 * 顺序是从字符串右边向左边计算，最右边的字符为0位。
 * 可重用。
 * </pre>
 * 
 * @author zhaigy
 * 
 */
public final class HexBitSet {
	static int[] TEST = { 0x1, 0x2, 0x4, 0x8 };
	static int[] TEST_FROM = { 0xf, 0xe, 0xb, 0x8 };
	static int[] TEST_TO = { 0x1, 0x3, 0x7, 0xf };
	static int[] TEST_COUNT = { 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4 };

	/** 探测给定位是否是1 */
	static public boolean testTrue(String hex, int bit) {
		int len = hex.length() << 2;// *4
		if (bit < 0 || bit >= len) {
			return false;
		}
		int idx = bit >> 2;// /4
		int offset = bit % 4;// %4
		char c = hex.charAt(hex.length() - idx - 1);
		int n = Character.digit(c, 16);
		return (n & TEST[offset]) == TEST[offset];
	}

	/** 探测给定位区间是否含有1。0<=from<=to。测试位包含to。 */
	static public boolean testTrue(String hex, int from, int to) {
		if (from == to) {
			return testTrue(hex, from);
		}
		int len = hex.length() << 2;// *4
		if (from >= len) {
			return false;
		}

		int fromIdx = from >> 2;// /4
		if (to >= len) {
			to = len - 1;
		}
		int toIdx = to >> 2;// /4

		for (int i = fromIdx; i <= toIdx; i++) {
			char c = hex.charAt(hex.length() - i - 1);
			int n = Character.digit(c, 16);
			if (i == fromIdx) {
				int fromOffset = from % 4;
				n = (n & TEST_FROM[fromOffset]);
			}
			if (i == toIdx) {
				int toOffset = to % 4;
				n = (n & TEST_TO[toOffset]);
			}
			if (n > 0) {
				return true;
			}
		}
		return false;
	}

	/** 探测给定区间存在1的个数 */
	static public int testTrueCount(String hex, int from, int to) {
		int len = hex.length() << 2;// *4
		if (from >= len) {
			return 0;
		}
		int fromIdx = from >> 2;// /4
		if (to >= len) {
			to = len - 1;
		}
		int toIdx = to >> 2;// /4
		int count = 0;
		for (int i = fromIdx; i <= toIdx; i++) {
			char c = hex.charAt(hex.length() - i - 1);
			int n = Character.digit(c, 16);
			if (i == fromIdx) {
				int fromOffset = from % 4;
				n = (n & TEST_FROM[fromOffset]);
			}
			if (i == toIdx) {
				int toOffset = to % 4;
				n = (n & TEST_TO[toOffset]);
			}
			if (n > 0) {
				count += TEST_COUNT[n];
			}
		}
		return count;
	}

	private String hex;

	public String getHex() {
		return hex;
	}

	public void setHex(String hex) {
		this.hex = hex;
	}

	/** 探测给定位是否是1 */
	public boolean testTrue(int bit) {
		return testTrue(hex, bit);
	}

	/** 探测给定位区间是否含有1。0<=from<=to。 */
	public boolean testTrue(int from, int to) {
		return testTrue(hex, from, to);
	}

	/** 探测给定区间存在1的个数 */
	public int testTrueCount(int from, int to) {
		return testTrueCount(hex, from, to);
	}
}
