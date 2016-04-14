package com.taobao.tair3.client.util;

import java.util.Arrays;

public final class ByteArray {
	public final byte[] array;
	private final int hash;
	
	public ByteArray(byte[] a)
	{
		array = a;
		hash = Arrays.hashCode(a);
	}
	
	@Override
	public int hashCode() {
		return hash;
	}
	
	public byte[] getBytes() {
		return array;
	}
	@Override
	public boolean equals(Object obj) {
		return obj instanceof ByteArray && Arrays.equals(array,((ByteArray)obj).array);
	}
}
