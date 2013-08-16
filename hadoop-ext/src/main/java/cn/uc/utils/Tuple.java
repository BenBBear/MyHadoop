package cn.uc.utils;

public class Tuple<K1, K2, V> {
	public final K1 k1;
	public final K2 k2;
	public final V v;

	public Tuple(K1 k1, K2 k2, V v) {
		this.k1 = k1;
		this.k2 = k2;
		this.v = v;
	}
}
