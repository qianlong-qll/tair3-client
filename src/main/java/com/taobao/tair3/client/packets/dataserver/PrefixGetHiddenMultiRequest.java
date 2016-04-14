package com.taobao.tair3.client.packets.dataserver;

import java.util.List;

import com.taobao.tair3.client.util.TairConstant;
public class PrefixGetHiddenMultiRequest extends PrefixGetMultiRequest {
	public PrefixGetHiddenMultiRequest(short ns, byte[] pkey, List<byte[]> skeys) {
		super(ns, pkey, skeys);
	}

	public static PrefixGetHiddenMultiRequest build(short ns, byte[] pkey, List<byte[]> skeys) {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (skeys == null || skeys.size() == 0) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		for (byte[] key : skeys) {
			if (key == null || (pkey.length + key.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
			}
		}
		PrefixGetHiddenMultiRequest request = new PrefixGetHiddenMultiRequest(ns, pkey, skeys);
		return request;
	}
}
