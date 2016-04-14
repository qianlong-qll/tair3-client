package com.taobao.tair3.client.rpc.net;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairRpcError;

public interface TairRpcPacket {
	public ChannelBuffer encode() throws TairRpcError;
	public int getChannelSeq();
	public Object getBody();
	public int getBodyLength();
	public void decodeBody() throws TairException;
	
	public boolean hasConfigVersion();
	
	public int decodeConfigVersion() throws TairException;
	public int decodeResultCode() throws TairException;
	
	public boolean assignBodyBuffer(ChannelBuffer in) throws TairRpcError;
}
