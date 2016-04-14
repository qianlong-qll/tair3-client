package com.taobao.tair3.client.rpc.protocol.tair2_3;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.rpc.net.TairRpcPacket;


public class PacketWrapper implements TairRpcPacket {
	
	private PacketHeader header;
	private Packet body = null;
	private ChannelBuffer lazyBody;
	
	private PacketWrapper (ChannelBuffer in) throws TairRpcError {
		header = new PacketHeader(in);
	}
	
	private PacketWrapper (int chid, Packet body) {
		this.body = body;
		header = new PacketHeader(chid, PacketManager.getPacketCode(body.getClass()));
	}
	
	public static PacketWrapper buildWithHeader(ChannelBuffer in) throws TairRpcError {
		return  new PacketWrapper(in);
	}
	
	public static PacketWrapper buildWithBody(int chid, Packet body) {
		return new PacketWrapper(chid, body);
	}
	
	public Packet getBody() {
		return body;
	}
	
	public void setBody(Packet body) {
		this.body = body;
		header.setPacketCode(PacketManager.getPacketCode(body.getClass()));
	}
	
	public boolean hasBody() {
		return body != null;
	}
	
	public ChannelBuffer encode() {
		
		ChannelBuffer buffer = ChannelBuffers.buffer(PacketHeader.HEADER_SIZE + body.size());
		
		header.encodeTo(buffer);
		body.encodeTo(buffer);
		header.encodeLength(buffer);
		
		buffer.resetReaderIndex();
		return buffer;
	}
	
	public int getBodyLength() {
		return header.getBodyLength();
	}
	
	public int getPacketCode() {
		return header.getPacketCode();
	}
	
	public int getHeaderLength() {
		return PacketHeader.HEADER_SIZE;
	}
	
	public int getChannelSeq() {
		return header.getChannelId();
	}
	
	public boolean assignBodyBuffer(ChannelBuffer in) throws TairRpcError {
		if (in.readableBytes() < getBodyLength()) {
			return false;
		}
		lazyBody = in.readSlice(getBodyLength());
		Class<? extends Packet> cls = PacketManager.getPacketClass(getPacketCode());
		if (cls == null) {
			throw new TairRpcError("unknow packet " + getPacketCode());
		}
		try {
			this.body = cls.newInstance();
		} catch (Exception e) {
			throw new TairRpcError(e);
		};
		return true;
	}
	
	public int decodeConfigVersion() throws TairException {
		return body.decodeConfigVersionFrom(lazyBody);
	}

	public void decodeBody() throws TairException {
		decodeBody(lazyBody);
	}
	
	private boolean decodeBody(ChannelBuffer in) {
		body.decodeFrom(in);
		return true;
	}

	public boolean hasConfigVersion() {
		return body.hasConfigVersion();
	}

	public int decodeResultCode() throws TairException {
		return body.decodeResultCodeFrom(lazyBody);
	}
}
