package cn.uc.hadoop.utils;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class LongBitSetTest {
	public static void main(String[] args) {
		LongBitSetTest test = new LongBitSetTest();
		test.testBytes();
		long t11 = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			LongBitSet lbs = new LongBitSet(5);
			lbs.set(0);
			lbs.set(1);
			lbs.set(2);
			lbs.set(3);
			lbs.set(4);
			Text text = new Text();
			text.set(LongBitSet.toBytes(lbs.getBitSet()));
			byte[] bb = text.getBytes();
			long bitset = LongBitSet.toBitSet(bb);
			int length = lbs.getCapacity();
			for (int j = 0; j < length; j++) {
				if (!lbs.get(j)) {
					continue;
				} else {

				}
			}
		}
		long t12 = System.currentTimeMillis();
		System.out.println(t12 - t11);

		long t21 = System.currentTimeMillis();
		Record record = new Record();
		for (int i = 0; i < 1000000; i++) {
			char[] index = new char[9];
			for (int j = 0; j < 3; j++) {
				index[2 * j + 1] = '\t';
			}
			index[0] = '1';
			index[2] = '1';
			index[4] = '1';
			index[6] = '1';
			index[8] = '1';
			String tt = new String(index);
			record.reset();
			record.setRecord(tt);
			record.setSplitChar('\t');
			int length = record.fieldSize();
			for (int k = 0; k < length; k++) {
				if (!record.equals("0")) {

				} else {

				}

			}
		}
		long t22 = System.currentTimeMillis();
		System.out.println(t22 - t21);

	}

	@Test
	public void testBytes() {
		LongBitSet lbs = new LongBitSet(5);
		lbs.set(0);
		lbs.set(1);
		lbs.set(2);
		lbs.set(3);
		lbs.set(4);
		Text text = new Text();
		text.set(LongBitSet.toBytes(lbs.getBitSet()));
		byte[] bb = text.getBytes();
		long bs = LongBitSet.toBitSet(bb);
		System.out.println(bs);
		// Assert.assertEquals(lbs.getBitSet(), bs);
	}

	@Test
	public void testCapacity() {
		{
			LongBitSet lbs = new LongBitSet(0);
			Assert.assertEquals(0, lbs.getCapacity());
		}
		{
			LongBitSet lbs = new LongBitSet(1);
			lbs.set(0);
			Assert.assertEquals(1, lbs.getCapacity());
		}
		for (int i = 2; i <= LongBitSet.MAX_SIZE; ++i) {
			LongBitSet lbs = new LongBitSet(i);
			lbs.set(0);
			lbs.set(1);
			Assert.assertEquals(i, lbs.getCapacity());
		}
	}

	@Test
	public void testCapacityText() {
		testCapacityTextEqual(0);

		testCapacityTextEqual(1);

		for (int i = 2; i <= LongBitSet.MAX_SIZE; ++i) {
			testCapacityTextEqual(i);
		}
	}

	void testCapacityTextEqual(int c) {
		LongBitSet lbs = new LongBitSet(c);
		byte[] bs = LongBitSet.toBytes(lbs.getBitSet());
		Text text = new Text(bs);
		byte[] bs2 = text.getBytes();
		LongBitSet lbs2 = new LongBitSet();
		lbs2.setBitSet(LongBitSet.toBitSet(bs2));
		Assert.assertEquals(lbs.getCapacity(), lbs2.getCapacity());
	}

	@Test
	public void testToBitSet() {
		LongBitSet lbs = new LongBitSet(9);
		lbs.set(8);
		long num = lbs.getBitSet();
		// System.out.println(num);
		byte[] bs = LongBitSet.toBytes(num);
		long num2 = LongBitSet.toBitSet(bs);
		Assert.assertEquals(num, num2);
	}

	@Test
	public void testValid() {
		{
			byte[] bs = { 49, 53, 52, 50, 49, 9, 48 };
			// 15421\t0
			Text text = new Text("15421\t0");
			bs = text.getBytes();
			if (LongBitSet.valid(bs)) {
				System.out.println("error!");
			}
			Assert.assertFalse(LongBitSet.valid(bs));
		}

		{
			LongBitSet lbs = new LongBitSet(9);
			lbs.set(8);
			byte[] bs = LongBitSet.toBytes(lbs.getBitSet());
			Assert.assertTrue(LongBitSet.valid(bs));
		}

		{
			byte[] bs = { -62, 1, 1 };
			Assert.assertFalse(LongBitSet.valid(bs));
		}
	}

}
