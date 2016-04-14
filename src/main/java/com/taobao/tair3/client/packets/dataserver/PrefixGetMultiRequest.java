package com.taobao.tair3.client.packets.dataserver;

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class PrefixGetMultiRequest extends AbstractRequestPacket {
	protected short namespace;
	protected byte[] pkey = null;
	protected List<byte[]> skeys = null;

	public PrefixGetMultiRequest(short ns, byte[] pkey, List<byte[]> skeys) {
		this.namespace = ns;
		this.pkey = pkey;
		this.skeys = skeys;
	}
	public static PrefixGetMultiRequest build(short ns, byte[] pkey, List<byte[]> skeys) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		for (byte[] key : skeys) {
			if (key == null || (pkey.length + key.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
			}
		}
		PrefixGetMultiRequest request = new PrefixGetMultiRequest(ns, pkey, skeys);
		return request;
	}
	@Override
	public void encodeTo(ChannelBuffer out) {
		if (pkey == null) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		out.writeByte((byte)0); // 1
		out.writeShort(namespace); // 2
		out.writeInt(skeys.size());
		for (byte[] skey : skeys) {
			if (skey == null) {
				throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
			}
			int keySize = pkey.length + PREFIX_KEY_TYPE.length;
			keySize <<= 22;
			keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
			encodeDataMeta(out);
			out.writeInt(keySize);
			out.writeBytes(PREFIX_KEY_TYPE);
			out.writeBytes(pkey);
			out.writeBytes(skey);
		}
	}

	public int size() {
		int s = 7;
		for (byte[] skey : skeys) {
			s+= (4 + 36 + pkey.length + skey.length + PREFIX_KEY_TYPE.length);
		}
		return s;
	}
	public short getNamespace() {
		return this.namespace;
	}
}
