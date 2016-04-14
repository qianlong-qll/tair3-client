package com.taobao.tair3.client.packets.dataserver;

import java.util.Map;
import org.jboss.netty.buffer.ChannelBuffer;
import com.taobao.tair3.client.TairClient.Counter;
import com.taobao.tair3.client.util.TairConstant;

public class BoundedPrefixIncDecRequest  extends PrefixIncDecRequest {
	public BoundedPrefixIncDecRequest(short ns, byte[] pkey, Map<byte[], Counter> skvs, int lowBound, int upperBound) {
		super(ns, pkey, skvs);
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
	
	public static BoundedPrefixIncDecRequest build(short ns, byte[] pkey, Map<byte[], Counter> skv, int lowBound, int upperBound) {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (skv == null || skv.size() == 0) {
			throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
		}
		for (Map.Entry<byte[], Counter> entry : skv.entrySet()) {
			byte[] skey = entry.getKey();
			if (skey == null || (pkey.length + skey.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
			}
			if (entry.getValue() == null) {
				throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
			}
		}
		if (lowBound > upperBound) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		BoundedPrefixIncDecRequest request = new BoundedPrefixIncDecRequest(ns, pkey, skv, lowBound, upperBound);
		return request;
	}
}
