package com.taobao.tair3.client.rpc.net;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.error.TairRpcError;
public interface TairRpcPacketFactory {

	public TairRpcPacket buildWithBody(int chid, Object body);
	public TairRpcPacket buildWithHeader(ChannelBuffer in) throws TairRpcError;
}
