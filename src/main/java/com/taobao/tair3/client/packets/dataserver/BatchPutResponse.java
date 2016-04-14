package com.taobao.tair3.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractResponsePacket;



public class BatchPutResponse extends AbstractResponsePacket {

	public boolean hasConfigVersion() {
		// TODO Auto-generated method stub
		return false;
	}

	public int decodeConfigVersionFrom(ChannelBuffer bb) {
		// TODO Auto-generated method stub
		return 0;
	}

}
