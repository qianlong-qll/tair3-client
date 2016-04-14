package com.taobao.tair3.client.packets.dataserver;
import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class GetHiddenRequest  extends AbstractRequestPacket {
	protected short namespace;
	protected byte[] pkey = null;
	protected byte[] skey = null;
	public GetHiddenRequest(short ns, byte[] pkey, byte[] skey) {
		this.namespace = ns;
		this.pkey = pkey;
		this.skey = skey;
	}
	public short getNamespace() {
		return this.namespace;
	}
	public static GetHiddenRequest build(short ns, byte[] pkey, byte[] skey) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (skey != null && ((pkey.length + skey.length + PREFIX_KEY_TYPE.length)> TairConstant.MAX_KEY_SIZE)) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		GetHiddenRequest request = new GetHiddenRequest(ns, pkey, skey);
		return request;
	}
	@Override
	public void encodeTo(ChannelBuffer out) {
		out.writeByte(0);
		out.writeShort(namespace);
		out.writeInt(1); // only one key
		int keySize = pkey.length;
		if (skey != null) {
			keySize += PREFIX_KEY_TYPE.length;
			keySize <<= 22;
			keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
		}
		encodeDataMeta(out);
		out.writeInt(keySize);
		if (skey != null) {
			out.writeBytes(PREFIX_KEY_TYPE);
		}
		out.writeBytes(pkey);
		if (skey != null) {
			out.writeBytes(skey);
		}
	}
	@Override
	public int size() {
		int s = 1 + 2 + 4 + 36 + 4 + pkey.length;
		if (skey != null) {
			s += skey.length;
			s += PREFIX_KEY_TYPE.length;
		}
		return s;
	}
}
