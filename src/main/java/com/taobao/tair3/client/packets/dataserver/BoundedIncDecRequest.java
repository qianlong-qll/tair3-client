package com.taobao.tair3.client.packets.dataserver;


import org.jboss.netty.buffer.ChannelBuffer;


import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.util.TairConstant;

public class BoundedIncDecRequest extends IncDecRequest {
    public BoundedIncDecRequest(short ns, byte[] pkey, byte[] skey, int count, int initValue, int expireTime, int lowBound, int upperBound) {
		super(ns, pkey, skey, count, initValue, expireTime);
		this.lowBound = lowBound;
		this.upperBound = upperBound;
	}

	protected int lowBound = Integer.MIN_VALUE;
    protected int upperBound = Integer.MAX_VALUE;
	
	@Override
	public void encodeTo(ChannelBuffer buffer) {
		super.encodeTo(buffer);
		buffer.writeInt(lowBound);
		buffer.writeInt(upperBound);
	}
	@Override
	public int size() {
		return super.size() + 4 + 4;
	}
	
	public static BoundedIncDecRequest build(short ns, byte[] pkey, byte[] skey, int value, int initValue, int lowBound, int upperBound, TairOption opt) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (skey != null && (pkey.length + skey.length + PREFIX_KEY_TYPE.length > TairConstant.MAX_KEY_SIZE)) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (lowBound > upperBound) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		BoundedIncDecRequest request = new BoundedIncDecRequest(ns, pkey, skey, value, initValue, opt.getExpire(), lowBound, upperBound);
		return request;
	}		
}
