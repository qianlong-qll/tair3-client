package com.taobao.tair3.client.packets.invalidserver;

/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class InvalidByProxyRequest extends AbstractRequestPacket {
	protected short namespace;
	protected byte[] pkey = null;
	protected byte[] skey = null;
	protected List<byte[]> keys = null;
	protected String group;
	protected int isSync = 1;

	public InvalidByProxyRequest(short namespace, byte[] pkey, byte[] skey, String group) {
		this.namespace = namespace;
		this.pkey = pkey;
		this.skey = skey;
		this.group = group;
		if (!this.group.endsWith("\0")) {
			this.group += "\0";
		}
		this.keys = null;
	}
	public InvalidByProxyRequest(short namespace, List<byte[]> keys, String group) {
		this.namespace = namespace;
		this.pkey = null;
		this.skey = null;
		this.group = group;
		if (!this.group.endsWith("\0")) {
			this.group += "\0";
		}
		this.keys = keys;
	}
	protected void encodeSingleKey(ChannelBuffer out) {
		out.writeByte((byte) 0); // 1
		out.writeShort(namespace); // 2
		out.writeInt(1); // 4, only one key
		int keySize = pkey.length;
		if (skey != null) {
			keySize += PREFIX_KEY_TYPE.length;
			keySize <<= 22;
			keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
		}
		encodeDataMeta(out); // 36
		out.writeInt(keySize); // 4
		if (skey != null) {
			out.writeBytes(PREFIX_KEY_TYPE);
		}
		out.writeBytes(pkey);
		if (skey != null) {
			out.writeBytes(skey);
		}
		out.writeInt(group.getBytes().length);
		out.writeBytes(group.getBytes());
		out.writeInt(isSync);
	}

	protected void encodeMultiKeys(ChannelBuffer out) {
		out.writeByte((byte)0); // 1
		out.writeShort(namespace); // 2
		out.writeInt(keys.size()); // 4
		for (byte[] key : keys) {
			encodeDataMeta(out); // 36
			int keySize = key.length;
			out.writeInt(keySize);
			out.writeBytes(key);
		}
		out.writeInt(group.getBytes().length);
		out.writeBytes(group.getBytes());
		out.writeInt(isSync);
	}
	@Override
	public void encodeTo(ChannelBuffer out) {
		if (keys != null) {
			encodeMultiKeys(out);
		}
		else {
			encodeSingleKey(out);
		}
	}
	
	@Override
	public int size() {
		int s = 0;
		if (keys == null) {
			s = 7 + 36 + 4 + pkey.length + 4 + group.getBytes().length + 4;
			if (skey != null) {
				s += skey.length;
			}
		}
		else {
			s += 7;
			for (byte[] key : keys) {
				s += (40 + key.length);
			}
		}
		s += (4 + group.getBytes().length + 4);
		return s;
	}
	
	public static InvalidByProxyRequest build(short ns, byte[] pkey, byte[] skey, String groupName) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (skey != null && ((pkey.length + skey.length + PREFIX_KEY_TYPE.length)> TairConstant.MAX_KEY_SIZE)) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		//if (!groupName.endsWith("\0")) {
		//	groupName += "\0";
		//}
		InvalidByProxyRequest request = new InvalidByProxyRequest(ns, pkey, skey, groupName);
		return request;
	}
	
	public static InvalidByProxyRequest build(short ns, List<byte[]> keys, String groupName) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (keys == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		for (byte[] key : keys) {
			if (key == null || key.length > TairConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
			}
		}
		InvalidByProxyRequest request = new InvalidByProxyRequest(ns, keys, groupName);
		return request;
	}
	@Override
	public short getNamespace() {
		return namespace;
	}
}
