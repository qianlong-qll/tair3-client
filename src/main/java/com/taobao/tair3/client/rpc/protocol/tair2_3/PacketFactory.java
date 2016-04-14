package com.taobao.tair3.client.rpc.protocol.tair2_3;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.rpc.net.TairRpcPacket;
import com.taobao.tair3.client.rpc.net.TairRpcPacketFactory;
public class PacketFactory implements TairRpcPacketFactory {
	private static PacketFactory instance = new PacketFactory();
	private PacketFactory() { }
	
	public static PacketFactory getInstance() {
		return instance;
	}

	public TairRpcPacket buildWithHeader(ChannelBuffer in) throws TairRpcError {
		TairRpcPacket rpcPacket =  PacketWrapper.buildWithHeader(in);
		return rpcPacket;
	}

	public TairRpcPacket buildWithBody(int chid, Object body) {
		return PacketWrapper.buildWithBody(chid, (Packet)body);
	}
}

