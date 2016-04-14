package com.taobao.tair3.client.packets.common;

import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import com.taobao.tair3.client.packets.AbstractResponsePacket;

public class BatchReturnResponse extends AbstractResponsePacket {
	protected String msg = null;
	protected int keyCount = 0;
	protected Map<byte[], Integer> keyCodeMap = null;
	
	public String getResultMessage() {
		return msg;
	}
	public int getCode() {
		return resultCode;
	}

	public Map<byte[], Integer> getKeyCodeMap() {
		return keyCodeMap;
	}
	public int getKeyCount() {
		return keyCount;
	}

	@Override
	public void decodeFrom(ChannelBuffer buffer) {
		resultCode = buffer.readInt();
		msg = readString(buffer);
		keyCount = buffer.readInt();
		if (keyCount > 0) {
			keyCodeMap = new HashMap<byte[], Integer>();
			//DataEntry de = new DataEntry();
			for (int i = 0; i < keyCount; ++i) {
				decodeMeta(buffer);
				int size = buffer.readInt();
				byte[] key = new byte[size];
				buffer.readBytes(key);
				int rc = buffer.readInt();
				keyCodeMap.put(key, rc);
			}
		}
		
	}

	private String readString(ChannelBuffer buffer) {
		int size = buffer.readInt();
		if (size <= 1) {
			return "";
		}
		else {
			byte[] array = new byte[size];
			buffer.readBytes(array);
			return new String(array, 0, size - 1);
		}
	}
	
	public boolean hasConfigVersion() {
		return true;
	}
	
	public int decodeConfigVersionFrom(ChannelBuffer bb) {
		return bb.readInt();
	}
}
