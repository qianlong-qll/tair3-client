package com.taobao.tair3.client.rpc.net;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.rpc.protocol.tair2_3.PacketHeader;

public class TairDecoder extends FrameDecoder{

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws TairRpcError {
		TairChannel tc = (TairChannel)channel.getAttachment();
		TairRpcPacket packet = tc.getCachedPacketWrapper();
		
		if (packet == null) {
			if (buffer.readableBytes() < PacketHeader.HEADER_SIZE) {
				return null;
			}
			
			packet = tc.getPacketFactory().buildWithHeader(buffer);
		}
		if (buffer.readableBytes() < packet.getBodyLength() ) {
			tc.setCachedPacketWrapper(packet);
			return null;
		} else {
			tc.setCachedPacketWrapper(null);
		}
		
		packet.assignBodyBuffer(buffer);
		return packet;
		
	}

}
