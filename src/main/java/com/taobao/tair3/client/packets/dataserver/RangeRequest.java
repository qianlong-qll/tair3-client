package com.taobao.tair3.client.packets.dataserver;
import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class RangeRequest extends AbstractRequestPacket {
	protected short type;
	protected short ns;
	protected byte[] pkey = null;
	protected byte[] startKey = null;
	protected byte[] endKey = null;
	
	protected int offset;
	protected int maxCount;
	
	public RangeRequest(short ns, byte[] pkey, byte[] startKey, byte[] endKey, int offset, int maxCount, short type) {
		this.ns = ns;
		this.pkey = pkey;
		this.startKey = startKey;
		this.endKey = endKey;
		this.offset = offset;
		this.maxCount = maxCount;
		this.type = type;
	}
	 @Override
	public void encodeTo(ChannelBuffer buffer) {
		buffer.writeByte((byte)0);
		buffer.writeShort(type);
		buffer.writeShort(ns);
		buffer.writeInt(offset);
		buffer.writeInt(maxCount);
		
		int keySize = pkey.length  + PREFIX_KEY_TYPE.length;
		keySize <<= 22;
		if (startKey != null) {
			keySize |= (pkey.length + startKey.length + PREFIX_KEY_TYPE.length);
		}
		else {
			keySize |= (pkey.length + PREFIX_KEY_TYPE.length);
		}
		encodeDataMeta(buffer);
		buffer.writeInt(keySize);
		buffer.writeBytes(PREFIX_KEY_TYPE);
		buffer.writeBytes(pkey);
		if (startKey != null) {
			buffer.writeBytes(startKey);
		}
		
		keySize = pkey.length  + PREFIX_KEY_TYPE.length;
		keySize <<= 22;
		if (endKey != null) {
			keySize |= (pkey.length + endKey.length + PREFIX_KEY_TYPE.length);
		}
		else {
			keySize |= (pkey.length + PREFIX_KEY_TYPE.length);
		}
		encodeDataMeta(buffer);
		buffer.writeInt(keySize);
		buffer.writeBytes(PREFIX_KEY_TYPE);
		buffer.writeBytes(pkey);
		if (endKey != null) {
			buffer.writeBytes(endKey);	
		}
	 }
	public int size() { 
		 int s = 1 + 2 + 2 + 4 + 4 + (40 + pkey.length + PREFIX_KEY_TYPE.length) + (40 + pkey.length + PREFIX_KEY_TYPE.length);
		 if (startKey != null) {
			 s += startKey.length;
		 }
		 if (endKey != null) {
			 s += endKey.length;
		 }
		 return s;
	 }
	public static RangeRequest build(short ns, byte[] pkey, byte[] start, byte[] end, int offset, int maxCount, short type, TairOption opt) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (offset < 0 || maxCount < 0) {
			throw new IllegalArgumentException(TairConstant.OPTION_NOT_AVAILABLE);
		}
		RangeRequest request = new RangeRequest(ns, pkey, start, end, offset, maxCount, type);
		return request;
	}
	public short getNamespace() {
		return this.ns;
	}
}
