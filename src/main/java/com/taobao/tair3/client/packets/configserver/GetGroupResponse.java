package com.taobao.tair3.client.packets.configserver;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractResponsePacket;
import com.taobao.tair3.client.util.TairUtil;

public class GetGroupResponse extends AbstractResponsePacket {
	
	private int cfgVer;
	private int copyCount;
	private int bucketCount;
	private List<SocketAddress> serverList = new ArrayList<SocketAddress>();
	private Map<String, String> configs = new HashMap<String, String>();
	private Set<SocketAddress> aliveNodes;

	public List<SocketAddress> getDataServers() {
		return serverList;
	}
	
	public Set<SocketAddress> getAliveServers() {
		return aliveNodes;
	}
	
	public int getCopyCount() {
		return copyCount;
	}
	
	public int getConfigVersion() {
		return cfgVer;
	}
	
	public int getBucketCount() {
		return bucketCount;
	}
	
	public Map<String, String> getConfigs() {
		return configs;
	}
	/**
	 * decode
	 */
	@Override
	public void decodeFrom(ChannelBuffer in) {
		bucketCount = in.readInt();
		copyCount 	= in.readInt();
		cfgVer 		= in.readInt();

		// get config items
		int count = in.readInt();
		for (int i = 0; i < count; i++) {
			configs.put(TairUtil.decodeString(in), 
						TairUtil.decodeString(in));
		}

		count = in.readInt();
		if (count > 0) {
			byte[] temp = new byte[count];
			in.readBytes(temp);
			byte[] result = TairUtil.deflate(temp);
			ByteBuffer buff = ByteBuffer.wrap(result);
			buff.order(ByteOrder.LITTLE_ENDIAN);

			List<SocketAddress> ss = new ArrayList<SocketAddress>();
			boolean valid = false;
			int c = 0;
			while (buff.hasRemaining()) {
				long sid = buff.getLong();
				if (!valid) {
					valid = sid != 0;
				}
				ss.add(TairUtil.cast2SocketAddress(sid));
				c++;
				if (c == bucketCount) {
					if (valid) {
						serverList.addAll(ss);
						ss = new ArrayList<SocketAddress>();
					}
					c = 0;
					valid = false;
				}
			}
		}
		
		aliveNodes = new HashSet<SocketAddress>();
		count = in.readInt();
		for(int i=0; i<count; i++)
			aliveNodes.add(TairUtil.cast2SocketAddress(in.readLong()));

		return ;
	}

	public boolean hasConfigVersion() {
		return false;
	}
	@Override
	public int decodeResultCodeFrom(ChannelBuffer bb) {
		return 0;
		//return bb.getInt(0);
	}
}
