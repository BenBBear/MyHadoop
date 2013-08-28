package cn.uc.utils;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

public class DoubleMap<K1,K2,V> {
	private Map<K1,Map<K2,V>> map ;
	public DoubleMap(){
		map = Maps.newHashMap();
	}
	public V get(K1 k1,K2 k2){
		Map<K2,V> temp = map.get(k1);
		if( temp == null){
			return null;
		}
		else{
			return temp.get(k2);
		}
	}
	public void put(K1 k1,K2 k2,V v){
		Map<K2,V> temp = map.get(k1);
		if( temp == null){
			temp = new HashMap<K2,V>();
			map.put(k1, temp);
		}
		temp.put(k2, v);
	}
	public boolean containsKey(K1 k1){
		return map.containsKey(k1);
	}
}
