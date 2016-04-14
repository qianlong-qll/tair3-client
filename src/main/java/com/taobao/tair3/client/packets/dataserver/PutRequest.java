/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair3.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairUtil;

public class PutRequest extends AbstractRequestPacket {
	protected short namespace;
	protected short version;
	protected int expired;
	protected byte[] pkey;
	protected byte[] skey;
	protected int keyFlag = 0;
	protected byte[] val;
	protected int valueFlag = 0;

	public PutRequest(short ns, byte[] pkey, byte[] skey, int keyFlag, byte[] val, int valueFlag,  short version, int expired) {
		this.namespace = ns;
		this.version = version;
		this.expired = expired;
		this.pkey = pkey;
		this.skey = skey;
		this.keyFlag = keyFlag;
		this.val = val;
		this.valueFlag = valueFlag;
	}
	@Override
	public void encodeTo(ChannelBuffer buffer) {
		buffer.writeByte((byte) 0); //1
		buffer.writeShort(namespace); //2
		buffer.writeShort(version); //2 
		buffer.writeInt(TairUtil.getDuration(expired)); //4
		//prefix
		int keySize = pkey.length;
		if (skey != null) {
			keySize += PREFIX_KEY_TYPE.length;
			keySize <<= 22;
			keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
		}
		//using static buffer
		encodeDataMeta(buffer, keyFlag);
		buffer.writeInt(keySize);
		if (skey != null) {
			//with prefix key
			buffer.writeBytes(PREFIX_KEY_TYPE);
		}
		buffer.writeBytes(pkey);
		if (skey != null) {
			buffer.writeBytes(skey);
		}

		encodeDataMeta(buffer, valueFlag);
		buffer.writeInt(val.length);
		buffer.writeBytes(val);
	}
	
	public int size() {
		int size = 9 + 40 + pkey.length + 40 + val.length;
		if (skey != null) {
			size += skey.length;
			size += PREFIX_KEY_TYPE.length;
		}
		return size;
	}


	public static PutRequest build(short ns, byte[] pkey, byte[] skey, int keyFlag, byte[] value, int valueFlag, TairOption opt) {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE || (skey != null && ((skey.length + pkey.length + PREFIX_KEY_TYPE.length)> TairConstant.MAX_KEY_SIZE))) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (value == null || value.length > TairConstant.MAX_VALUE_SIZE) {
			throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
		}
		//we must create the instance
		PutRequest request = new PutRequest(ns, pkey, skey, keyFlag, value, valueFlag, opt.getVersion(), opt.getExpire());
		return request;
	}
	public short getNamespace() {
		return this.namespace;
	}
}
