package cn.uc.hadoop.utils;

import java.util.Map;

import org.apache.hadoop.io.Text;

public final class MyRecord {
	interface Function<F, T> {
		F call(T input);
	}

	private static final char FIELD_SEPARATE = '`';

	MyRecord(Text text) {

	}

	// TODO
	// String get(int idx) {
	// return null;
	// }

	void del(int idx) {

	}

	<F, T> void func(int idx, Function<F, T> f, int[] idxs) {

	}

	int getFieldNumb() {
		return 0;
	}

	/** 是func的特例 */
	void map(int idx, Map map, int... idxs) {

	}

	/** 是map的特例 */
	void set(int idx, int... idxs) {
	}

	void set(int idx, String str) {

	}

	Text toText() {
		return null;
	}
}
