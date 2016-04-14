package com.taobao.tair3.client.packets.invalidserver;
import com.taobao.tair3.client.util.TairConstant;
/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */



public class HideByProxyRequest extends InvalidByProxyRequest {
	//Only two cases:
	//1) pkey != null && skey == null hideByProxy
	//2) pkey != null && skey != null prefixHideByProxy
	public HideByProxyRequest(short ns, byte[] pkey, byte[] skey, String group) {
		super(ns, pkey, skey, group);
	}

	public static HideByProxyRequest build(short ns, byte[] pkey, byte[] skey, String groupName) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (skey != null && ((pkey.length + skey.length + PREFIX_KEY_TYPE.length)> TairConstant.MAX_KEY_SIZE)) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		HideByProxyRequest request = new HideByProxyRequest(ns ,pkey, skey, groupName);
		return request;
	}
}
