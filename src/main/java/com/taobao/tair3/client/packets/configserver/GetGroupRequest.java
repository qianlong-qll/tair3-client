package com.taobao.tair3.client.packets.configserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;


public class GetGroupRequest extends AbstractRequestPacket {
	
    private byte[] group;
    private int    cfgVer;
    
    public GetGroupRequest(String groupName, int version) {
    	this.group = groupName.getBytes();
    	this.cfgVer = version;
    }
    
    @Override
    public void encodeTo(ChannelBuffer out) {
		out.writeInt(cfgVer);
		out.writeInt(group.length);
		out.writeBytes(group);
	}
    
	public int size() {
		return 4 + 4 + group.length;
	}

	@Override
	public short getNamespace() {
		return 0;
	}

 
}
