package com.taobao.tair3.client.packets;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.Result;



public abstract class AbstractResponsePacket extends AbstractPacket {
	protected int resultCode = -1;
	
	public int decodeConfigVersion(ChannelBuffer buff) {
		return buff.readInt();
	}
	
	public <T> void decodeMeta(ChannelBuffer buff, Result<T> r) {
		buff.readByte(); // isMerged
		buff.readInt(); // area
		buff.readShort(); // serverFlag	
		buff.readShort(); // magic code
		buff.readShort(); // check sum
		buff.readShort(); // key size
		Short version = buff.readShort();
		buff.readInt();	// pad size	
		buff.readInt(); // value size
		Byte flag = buff.readByte();
		Integer cdate = buff.readInt(); // cdate
		Integer mdate = buff.readInt(); // mdate
		Integer edate = buff.readInt(); // edate
		r.setVersion(version);
		r.setFlag(flag);
		r.setExpire(edate);
		r.setModifyTime(mdate);
		r.setCreateTime(cdate);
	}
	public void decodeMeta(ChannelBuffer buff) {
		buff.readByte(); // isMerged
		buff.readInt(); // area
		buff.readShort(); // serverFlag	
		buff.readShort(); // magic code
		buff.readShort(); // check sum
		buff.readShort(); // key size
		buff.readShort();
		buff.readInt();	// pad size	
		buff.readInt(); // value size
		buff.readByte();
		buff.readInt(); // cdate
		buff.readInt(); // mdate
		buff.readInt(); // edate
	}

	public byte[] getKey() {
		return null;
	}
	
	public void setCode(int code) {
		resultCode = code;
	}
	
	public int getCode () {
		return resultCode;
	}
	
	public int decodeConfigVersionFrom(ChannelBuffer bb) {
		return bb.readInt();
	}
	public int decodeResultCodeFrom(ChannelBuffer bb) {
		int r = bb.getInt(4);
		return r;
	}
	
}
