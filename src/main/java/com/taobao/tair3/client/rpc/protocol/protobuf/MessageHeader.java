package com.taobao.tair3.client.rpc.protocol.protobuf;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.error.TairRpcError;

public class MessageHeader {

	private int		channelId;
	private byte[] 	messageType;
	private int 	messageLength = 0;
	
	private MessageHeader(int chid) {
		this.channelId = chid;
	}
	
	public MessageHeader(ChannelBuffer bb) throws TairRpcError {
		this.decode(bb);
	}
	
	public MessageHeader(int chid, byte[] messageType) {
		this(chid);
		this.channelId = chid;
		this.messageType = messageType;
	}
	
	public void encodeTo(ChannelBuffer bb) {	
		bb.writeInt(Integer.reverseBytes(channelId));
		//TODO: short is ok, int just for debug
		bb.writeInt(Integer.reverseBytes(messageType.length));
		bb.writeBytes(messageType);
		bb.writeInt(Integer.reverseBytes(messageLength));
	}	
	
	public void decode(ChannelBuffer bb) throws TairRpcError {
		/*
		magicCode = bb.readInt();
		if (magicCode != PACKET_MAGIC_CODE) {
			throw new TairRpcError("stream error, magic code not match");
		}*/
		channelId = Integer.reverseBytes(bb.readInt());
		messageType = new byte[Integer.reverseBytes(bb.readInt())];
		bb.readBytes(messageType);
		messageLength = Integer.reverseBytes(bb.readInt());
	}	
	
	public void encodeLength(ChannelBuffer buffer) {
		int headerLength = 12 + messageType.length;
		int bodyLength =  buffer.writerIndex() - headerLength;
		buffer.setInt(headerLength - 4,Integer.reverseBytes(bodyLength));
	}
	
	public int getMessageLength() {
		return messageLength;
	}
	
	public int getChannelSeq() {
		return channelId;
	}
	
	public byte[] getMessageType() {
		return messageType;
	}
}
