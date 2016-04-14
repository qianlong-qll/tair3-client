package com.taobao.tair3.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ResultMap<K, V> extends Result<Map<K, V>> implements Map<K, V> {
	protected Map<K, V> map = null;
	
	public ResultMap() {
		this.map = new HashMap<K, V>();
		this.setResult(map);
		this.setCode(ResultCode.INIT);
	}
	
	
	public ResultMap(TreeMap<K, V> map) {
		this.map = map;
		this.setResult(map);
		this.setCode(ResultCode.INIT);
	}
	
	
	public ResultMap(int initialCapacity) {
		this.map = new HashMap<K, V>();
		this.setResult(map);
		this.setCode(ResultCode.INIT);
	}

	public int size() {
		return map.size();
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	public V get(Object key) {
		return map.get(key);
	}

	public V put(K key, V value) {
		return map.put(key, value);
	}

	public V remove(Object key) {
		return map.remove(key);
	}

	public void putAll(Map<? extends K, ? extends V> m) {
		map.putAll(m);
	}

	public void clear() {
		map.clear();
	}

	public Set<K> keySet() {
		return map.keySet();
	}

	public Collection<V> values() {
		return map.values();
	}

	public Set<Map.Entry<K, V>> entrySet() {
		return map.entrySet();
	}

	public void setResultCode(Set<ResultCode> codes) {
		if (codes.size() == 0) {
			this.setCode(ResultCode.FAILED);
		} else if (codes.contains(ResultCode.PART_OK)) {
			this.setCode(ResultCode.PART_OK);
		}
		else if (codes.size() == 1) {
			for (ResultCode code : codes) {
				this.setCode(code);
			}
		}
		else {
			if (codes.contains(ResultCode.OK)) {
				this.setCode(ResultCode.PART_OK);
			}
			else {
				this.setCode(ResultCode.FAILED);
			}
		}
	}

}
