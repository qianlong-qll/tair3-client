package com.taobao.tair3.client.rpc.protocol.protobuf;

import org.jboss.netty.buffer.ChannelBuffer;

import com.google.protobuf.Message;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.rpc.net.TairRpcPacket;
import com.taobao.tair3.client.rpc.net.TairRpcPacketFactory;

public class MessageFactory implements TairRpcPacketFactory {

	private static MessageFactory instance = new MessageFactory();
	
	private MessageManager manager = null; 

	private MessageFactory() {
		try {
			manager = new MessageManager();
		} catch (Exception e) {
			throw new RuntimeException(new TairException(e));
		} 
	}

	public static TairRpcPacketFactory getInstance() {
		return instance;
	}

	public TairRpcPacket buildWithHeader(ChannelBuffer in) throws TairRpcError {
		return MessageWrapper.buildWithHeader(in, manager);
	}

	public TairRpcPacket buildWithBody(int chid, Object body) {
		return MessageWrapper.buildWithBody(chid, (Message) body, manager);
	}
}
