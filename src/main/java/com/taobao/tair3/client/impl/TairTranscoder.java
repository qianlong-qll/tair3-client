package com.taobao.tair3.client.impl;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class TairTranscoder {
	
	public static TairTranscoder defaultTranscoder = new TairTranscoder();

	ChannelBuffer encodeTo(Object obj) throws RuntimeException {
		return ChannelBuffers.wrappedBuffer((byte[])obj);
	}
	
	public Object decodeTo(ChannelBuffer buffer) throws RuntimeException {
		return buffer.array();
	}
}
