package com.taobao.tair3.client.rpc.protocol.tair2_3;

import org.jboss.netty.buffer.ChannelBuffer;


public interface Packet {
	
	public void encodeTo(ChannelBuffer bb);
	
	public int getPacketCode();
	
	public void decodeFrom(ChannelBuffer bb);
	
	public int size();
	
	public boolean hasConfigVersion();
	
	public int decodeConfigVersionFrom(ChannelBuffer bb);
	public int decodeResultCodeFrom(ChannelBuffer bb);

}
