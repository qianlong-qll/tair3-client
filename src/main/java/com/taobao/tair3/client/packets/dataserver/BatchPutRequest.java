package com.taobao.tair3.client.packets.dataserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.ByteArray;

public class BatchPutRequest extends AbstractRequestPacket {
	protected short namespace;
	protected short version;
	protected int expired;
	protected List<byte[]> keySet = new ArrayList<byte[]>();
	protected List<byte[]> valSet = new ArrayList<byte[]>();
	public void setNamespace(short namespace) {
		this.namespace = namespace;
	}
	public short getNamespace() {
		return this.namespace;
	}
	public void setVersion(short version) {
		this.version = version;
	}
	public void setExpired(int expired) {
		this.expired = expired;
	}
	
	void addKey(byte[] key) {
		this.keySet.add(key);
	}
	void addVal(byte[] val) {
		this.valSet.add(val);
	}
	public void encodeTo(ChannelBuffer buffer) {
		buffer.writeByte((byte) 0); // 1
		buffer.writeShort(namespace); // 2
		buffer.writeShort(version); //2 
		buffer.writeInt(expired); //4

		buffer.writeInt(keySet.size()); //4
		for (byte[] key : keySet) {
			if (key == null) {
				throw new IllegalArgumentException("key was null, BatchPutRequest::encodeTo");
			}
			encodeDataMeta(buffer); // 36
			buffer.writeInt(key.length);  // 4
			buffer.writeBytes(key);
		}
		
		buffer.writeInt(valSet.size()); //4
		for (byte[] val : valSet) {
			encodeDataMeta(buffer); // 36
			buffer.writeInt(val.length);  // 4
			buffer.writeBytes(val);
		}
	}

	public int size() {
		int s = 9;
		for (byte[] k : keySet) {
			s += 36 + 4 + k.length;
		}
		s += 4;
		for (byte[] v : valSet) {
			s += 36 + 4 + v.length;
		}
		return s;
	}
	public List<byte[]> getKeySet()  {
		return keySet;
	}
	public static BatchPutRequest build(short ns, byte[] key, byte[] val, TairOption opt) throws IllegalArgumentException {
		BatchPutRequest req = new BatchPutRequest();
		req.setExpired(opt.getExpire());
		req.setVersion(opt.getVersion());
		req.setNamespace(ns);
		req.addKey(key);
		req.addVal(val);
		return req;
	}
	public static BatchPutRequest build(short ns, Map<ByteArray, byte[]> kv, TairOption opt) throws IllegalArgumentException {
		BatchPutRequest req = new BatchPutRequest();
		req.setExpired(opt.getExpire());
		req.setVersion(opt.getVersion());
		req.setNamespace(ns);
		Iterator<Map.Entry<ByteArray, byte[]>> it = kv.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<ByteArray, byte[]> entry = it.next();
			byte[] key = entry.getKey().getBytes();
			byte[] val = entry.getValue();
			req.addKey(key);
			req.addVal(val);
		}
		return req;
	}
}