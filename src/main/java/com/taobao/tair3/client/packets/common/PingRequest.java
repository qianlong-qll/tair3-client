package com.taobao.tair3.client.packets.common;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;

public class PingRequest extends AbstractRequestPacket {
	private int configVersion = 0;
	private int value = 0;
	
	@Override
	public void encodeTo(ChannelBuffer bb) {
		bb.writeInt(configVersion);
		bb.writeInt(value);
	}
	
	public int size() {
		return 8;
	}
	
	public int getConfigVersion() {
		return this.configVersion;
	}
	
	public void setConfigVersion(int configVersion) {
		this.configVersion = configVersion;
	}
	
	public int getValue() {
		return this.value;
	}
	
	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public short getNamespace() {
		return 0;
	}
}