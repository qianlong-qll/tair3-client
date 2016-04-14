package com.taobao.tair3.client.packets;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.taobao.tair3.client.rpc.protocol.tair2_3.Packet;
import com.taobao.tair3.client.rpc.protocol.tair2_3.PacketManager;


public abstract class AbstractPacket implements Packet {
	protected static final short NAMESPACE_MAX = Short.MAX_VALUE;
	
	protected static final int METADATA_SIZE = 36;
	protected static final ChannelBuffer COMMON_META = ChannelBuffers.buffer(METADATA_SIZE);
	protected static final ChannelBuffer COUNTER_META = ChannelBuffers.buffer(METADATA_SIZE);
	//flag
	protected static final byte[] PREFIX_KEY_TYPE = new byte[2];
    protected Object context = null;
	static {
		PREFIX_KEY_TYPE[1] = (byte) ((12 << 1) & 0xFF);
		PREFIX_KEY_TYPE[0] = (byte) (((12 << 1) >> 8) & 0xFF);
		
		COMMON_META.writeZero(METADATA_SIZE);
		COUNTER_META.writeZero(23);
		COUNTER_META.writeByte(1);
		COUNTER_META.writeZero(12);
	}
	protected void encodeDataMeta(ChannelBuffer out) {
		out.writeBytes(COMMON_META, 0, METADATA_SIZE);
	}
	 
	protected void encodeDataMeta(ChannelBuffer out, int flag) {
		if (flag == 0) {
			out.writeBytes(COMMON_META, 0, METADATA_SIZE);
		}
		else {
			out.writeBytes(COUNTER_META, 0, METADATA_SIZE);
		}
	}
	protected void skipMetas(ChannelBuffer buff) {
		buff.readByte();
		buff.readInt();
		buff.readShort();
	}

	protected void encodeDataMeta(ChannelBuffer out, short version, int expire) {	
		out.writeZero(13);
		out.writeShort(version);
		out.writeInt(0);
		out.writeInt(0);
		out.writeByte((byte)0);
		out.writeInt(0);
		out.writeInt(0);
		out.writeInt(expire);
	}
	public int getPacketCode() {
		return PacketManager.getPacketCode(this.getClass());
	}
	
	public int size() {
		return 0;
	}
	
	public void decodeFrom(ChannelBuffer bb) {
		throw new UnsupportedOperationException("decode not implement " + getClass().getName());
	}
	
	public void encodeTo(ChannelBuffer bb) {
		throw new UnsupportedOperationException("encode not implement " + getClass().getName());
	}

    public Object  getContext() {
        return context;
    }

    public void setContext(Object context) {
        this.context = context;
    } 
    public int decodeResultCodeFrom(ChannelBuffer bb) {
    	throw new UnsupportedOperationException("decode resultcode not implement " + getClass().getName());
    }
   
}
