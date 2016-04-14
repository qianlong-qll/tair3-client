package com.taobao.tair3.client.packets.invalidserver;

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;
public class InvalidByProxyMultiRequest extends AbstractRequestPacket {
	protected short namespace;
	//only 2 cases:
	//1) keys != null, invlaid
	//2) pkey != null && keys != null, prefixInvalid
	protected byte[] pkey = null;
	protected List<byte[]> keys = null;
	protected String group = null;
	protected int isSync = 1;


	public InvalidByProxyMultiRequest(short ns, List<byte[]> keys, String group) {
		this.namespace = ns;
		this.keys = keys;
		this.pkey = null;
		this.group = group;
		if (!this.group.endsWith("\0")) {
			this.group += "\0";
		}
	}
	public InvalidByProxyMultiRequest(short ns, byte[] pkey, List<byte[]> skeys, String group) {
		this.namespace = ns;
		this.pkey = pkey;
		this.keys = skeys;
		this.group = group;
		if (!this.group.endsWith("\0")) {
			this.group += "\0";
		}
	}
	
	public static InvalidByProxyMultiRequest build(short ns, List<byte[]> keys, String groupName) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (keys == null ) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		for (byte[] key : keys) {
			if (key == null || key.length > TairConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
			}
		}
		InvalidByProxyMultiRequest request = new InvalidByProxyMultiRequest(ns, keys, groupName);
		return request;
	}
	public static InvalidByProxyMultiRequest build(short ns, byte[] pkey, List<byte[]> skeys, String groupName) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (skeys == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		for (byte[] skey : skeys) {
			if (skey == null || (skey.length + pkey.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
			}
		}
		InvalidByProxyMultiRequest request = new InvalidByProxyMultiRequest(ns, pkey, skeys, groupName);
		return request;
	}
	@Override
	public void encodeTo(ChannelBuffer out) {
		out.writeByte((byte)0); // 1
		out.writeShort(namespace); // 2
		out.writeInt(keys.size()); // 4
		for (byte[] key : keys) {
			encodeDataMeta(out); // 36
			int keySize = key.length;
			if (pkey != null) {
				keySize = pkey.length + PREFIX_KEY_TYPE.length;
				keySize <<= 22;
				keySize |= (pkey.length + key.length + PREFIX_KEY_TYPE.length);
			}
			out.writeInt(keySize);
			if (pkey != null) {
				out.writeBytes(PREFIX_KEY_TYPE);
				out.writeBytes(pkey);
			}
			out.writeBytes(key);
		}
		out.writeInt(group.getBytes().length);
		out.writeBytes(group.getBytes());
		out.writeInt(isSync);
	}
	@Override
	public int size() {
		int s = 1 + 2 + 4;
		for (byte[] key : keys) {
			s += ( key.length + 40);
			if (pkey != null) {
				s += pkey.length;
				s += PREFIX_KEY_TYPE.length;
			}
		}
		return s + 4 + group.getBytes().length + 4;
	}
	@Override
	public short getNamespace() {
		return namespace;
	}
}
