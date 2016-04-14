package com.taobao.tair3.client.packets.configserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.packets.dataserver.HideRequest;
import com.taobao.tair3.client.util.TairConstant;

public class QueryInfoRequest extends AbstractRequestPacket {

    private int qtype; 
    private String groupName;
    long server_id = 0;
    
    public QueryInfoRequest (int qtype, String group, long server_id) {
    	this.qtype = qtype;
    	this.groupName = group;
    	this.server_id = server_id;
    }
    @Override
    public void encodeTo(ChannelBuffer out) {
		out.writeInt(qtype);
		out.writeInt(groupName.length());
		out.writeBytes(groupName.getBytes());
		out.writeLong(server_id);
	}
    
	public int size() {
		return 4 + 8 + 4 + groupName.length();
	}
	
	public static QueryInfoRequest build(int qtype, String group, long server_id) throws IllegalArgumentException {
		 
		if (group == null || group.isEmpty() || server_id < 0) {
			throw new IllegalArgumentException(TairConstant.ITEM_VALUE_NOT_AVAILABLE);
		}
		QueryInfoRequest request = new QueryInfoRequest(qtype, group, server_id);
		return request;
	}
	@Override
	public short getNamespace() {
		return 0;
	}
}
