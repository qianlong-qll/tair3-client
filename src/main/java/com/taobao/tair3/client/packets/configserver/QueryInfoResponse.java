package com.taobao.tair3.client.packets.configserver;

import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractResponsePacket;

public class QueryInfoResponse extends AbstractResponsePacket {

	Map<String, String> infoMap = new HashMap<String, String> ();
	public QueryInfoResponse() {
		infoMap.size();
	}
	public boolean hasConfigVersion() {
		return false;
	}
	
	public Map<String, String> getInfoMap() {
		return infoMap;
	}
	private String readString(ChannelBuffer in) {
		int len = in.readInt();
        if (len <= 1) {
            return "";
        } else {
            byte[] b = new byte[len];
            in.readBytes(b);
            return new String(b, 0, len - 1);
        }
	}

	@Override
	public void decodeFrom(ChannelBuffer in) {
		int count = in.readInt();
		for (int i = 0; i < count; i++) {
			String name = readString(in);
			String value = readString(in);
			infoMap.put(name, value);
		}
	}
	@Override
	public int decodeResultCodeFrom(ChannelBuffer bb) {
		return 0;
		//return bb.getInt(0);
	}

}
