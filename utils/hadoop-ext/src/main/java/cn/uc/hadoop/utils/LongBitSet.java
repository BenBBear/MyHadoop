package cn.uc.hadoop.utils;

/**
 * <pre>
 * 使用一个long表示的BitSet。最长表示56个位长。
 * 第一个字节存储长度，不使用最高位。
 * 为了和UTF8编码区分，长度的编码的高2位是11.
 * </pre>
 */
public class LongBitSet {
	private static final long VALUE_MARK = 0x00ffffffffffffffL;
	static final public int MAX_SIZE = 56;// 7*8

	public static long capacity(long bitset) {
		return bitset >> MAX_SIZE;
	}

	public static void main(String[] args) {
		byte b = -1;
		System.out.println(Integer.toBinaryString(b));
		System.out.println((byte) (b & 0x81));
		System.out.println(Integer.toBinaryString((byte) (b & 0x81)));
	}

	/**
	 * {@link #toBytes(long)} 反解
	 * 
	 * @param bb
	 * @return
	 */
	public static long toBitSet(byte[] bb) {
		if (bb.length <= 1) {
			return 0;
		}
		int capacity = (bb[0] & 0x3f);

		long v = 0;
		for (int i = 1; i < bb.length; ++i) {
			v <<= 8;
			v |= (0xff & bb[i]);
		}
		v |= (((long) capacity) << MAX_SIZE);
		return v;
	}

	/**
	 * 编码：按顺序把capacity,value的各个字节写下来。其中capacity只会用后6位，前两位设定为11.
	 * 
	 * @param bitset
	 * @return 编码后的字节。
	 */
	public static byte[] toBytes(long bitset) {
		long v = value(bitset);
		int c = (int) capacity(bitset);
		if (c == 0) {
			return new byte[] { 0 };
		}
		int byteNum = (c + 8 - 1) / 8;
		byte[] bb = new byte[byteNum + 1];

		bb[0] = (byte) (c | 0xc0);// 1100 0000

		for (int i = 0; i < byteNum; ++i) {
			bb[byteNum - i] = (byte) v;
			v >>= 8;
		}

		return bb;
	}

	public static boolean valid(byte[] bb) {
		if (bb.length <= 1) {
			return 0 == bb[0];
		}
		int c = bb[0];
		if ((c & 0xc0) != 0xc0) {// 高位用1填充了，一定是负值
			return false;
		}
		c &= 0x3f;
		if (c > MAX_SIZE) {
			return false;
		}
		if ((c + 7) / 8 + 1 != bb.length) {
			return false;
		}
		return true;
	}

	public static long value(long bitset) {
		return bitset & VALUE_MARK;
	}

	long bs;

	public LongBitSet() {
		this(MAX_SIZE);
	}

	public LongBitSet(int capacity) {
		reset(capacity);
	}

	public boolean get(int bit) {
		if (bit < 0 || bit >= MAX_SIZE) {
			throw new IllegalArgumentException("bit must in [0 " + MAX_SIZE
					+ ")");
		}
		if (bit >= getCapacity()) {
			return false;
		}
		return (bs & (1L << bit)) != 0;
	}

	public long getBitSet() {
		return bs;
	}

	public int getCapacity() {
		return (int) capacity(bs);
	}

	public void reset(int capacity) {
		if (capacity < 0 || capacity > MAX_SIZE) {
			throw new IllegalArgumentException("capacity must in [0 "
					+ MAX_SIZE + "]");
		}
		bs = ((long) capacity) << MAX_SIZE;
	}

	public void set(int bit) {
		if (bit < 0 || bit >= MAX_SIZE) {
			throw new IllegalArgumentException("bit must in [0 " + MAX_SIZE
					+ ")");
		}
		// if (bit >= getCapacity()) {
		// return false;
		// }
		bs |= (1L << bit);
	}

	public void setBitSet(long bs) {
		this.bs = bs;
	}

	public void setCapacity(int capacity) {
		if (capacity < 0 || capacity > MAX_SIZE) {
			throw new IllegalArgumentException("capacity must in [0 "
					+ MAX_SIZE + "]");
		}
		bs = (((long) capacity) << MAX_SIZE) | value(bs);
	}
}
