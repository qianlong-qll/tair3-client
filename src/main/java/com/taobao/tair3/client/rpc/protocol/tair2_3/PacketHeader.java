package com.taobao.tair3.client.rpc.protocol.tair2_3;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.error.TairRpcError;

public class PacketHeader {
	
	public static final int PACKET_MAGIC_CODE = 0x6d426454;
	
	public final static int HEADER_SIZE = 16;
	public final static int LEN_POS = 12;
	
	protected int magicCode = PACKET_MAGIC_CODE;
	protected int chid;
	protected int packetCode = 0;
	protected int bodyLength = 0;
	
	private PacketHeader(int chid) {
		this.chid = chid;
	}
	
	public PacketHeader(ChannelBuffer bb) throws TairRpcError {
		this.decode(bb);
	}
	
	public PacketHeader(int chid, int packetCode) {
		this(chid);
		this.packetCode = packetCode;
	}
	
	public void setPacketCode(int packetCode) {
		this.packetCode = packetCode;
	}
	
	public void encodeTo(ChannelBuffer bb) {	 
		bb.writeInt(magicCode);
		bb.writeInt(chid);
		bb.writeInt(packetCode);
		bb.writeInt(bodyLength); //len
	}	

	public void decode(ChannelBuffer bb) throws TairRpcError {
		magicCode = bb.readInt();
		if (magicCode != PACKET_MAGIC_CODE) {
			throw new TairRpcError("stream error, magic code not match");
		}
		chid = bb.readInt();
		packetCode = bb.readInt();
		bodyLength = bb.readInt();
		if (bodyLength != 12) {
			return;
		}
		
	}	
	
	public void encodeLength(ChannelBuffer buffer) {
		buffer.setInt(PacketHeader.LEN_POS, buffer.writerIndex() - PacketHeader.HEADER_SIZE);
	}
	
	public int getBodyLength() {
		return bodyLength;
	}
	
	public int getPacketCode() {
		return packetCode;
	}
	
	public int getChannelId() {
		return chid;
	}
}
