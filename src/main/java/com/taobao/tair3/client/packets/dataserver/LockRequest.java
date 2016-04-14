package com.taobao.tair3.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class LockRequest extends AbstractRequestPacket {
	public static final int LOCK_STATUS = 1;
	public static final int LOCK_VALUE = 2;
	public static final int UNLOCK_VALUE = 3;
	protected short namespace;
	private int lockType = LOCK_VALUE;
	private byte[] key = null;
	public LockRequest(short ns, byte[] key, int lockType) {
		this.namespace = ns;
		this.key = key;
		this.lockType = lockType;
	}

	public short getNamespace() {
		return this.namespace;
	}
	@Override
	public void encodeTo(ChannelBuffer buffer) {
		buffer.writeShort(namespace); // 2
		buffer.writeInt(lockType); //4
		encodeDataMeta(buffer); // 36
		buffer.writeInt(key.length); //4
		buffer.writeBytes(key);
	}
	
	public int size() {
		return 2 + 4 + 36 + 4 + key.length;
	}
	
	public static LockRequest build(short ns, byte[] key, int lockType) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (key == null || key.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		LockRequest request = new LockRequest(ns, key, lockType);
		return request;
	}
}
