package com.taobao.tair3.client.packets.dataserver;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class GetRequest extends AbstractRequestPacket {
	protected short namespace;
	//only two cases:
	//1) pkey !=null  && keys == null  get, prefixGet
	//2) keys != null && pkey == null  batchGet
	protected byte[] pkey = null;
	protected byte[] skey = null;
	protected List<byte[]> keys = null;

	public GetRequest(short namespace, byte[] pkey, byte[] skey) {
		this.namespace = namespace;
		this.pkey = pkey;
		this.skey = skey;
	}
	
	public GetRequest(short namespace, List<byte[]> keys) {
		this.namespace = namespace;
		this.keys = keys;
	}

	public void setNamespace(short namespace) {
		this.namespace = namespace;
	}
	 public short getNamespace() {
	    	return namespace;
	    }

	@Override
	public void encodeTo(ChannelBuffer out) {
		out.writeByte((byte)0); // 1
		out.writeShort(namespace); // 2
		//single key
		if (pkey != null && keys == null) {
			out.writeInt(1); // 4
			int keySize = pkey.length ;
			if (skey != null) {
				keySize += PREFIX_KEY_TYPE.length;
				keySize <<= 22;
				keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
			}
			encodeDataMeta(out); // 36
			out.writeInt(keySize);  // 4
			if (skey != null) {
				out.writeBytes(PREFIX_KEY_TYPE);
			}
			out.writeBytes(pkey);
			if (skey != null) {
				out.writeBytes(skey);
			}
		}
		else if (keys != null) {
			out.writeInt(keys.size()); // 4
			for (byte[] key : keys) {
				encodeDataMeta(out); // 36
				out.writeInt(key.length);
				out.writeBytes(key);
			}
		}
		else {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
	}

	public int size() {
		int s = 7;
		if (pkey != null && keys == null) {
			s += (40 + pkey.length);
			if (skey != null) {
				s += skey.length;
				s += PREFIX_KEY_TYPE.length;
			}
		} 
		else if (keys != null) {
			for (byte[] key : keys) {
				if (key != null) {
					s += (40 + key.length);
				}
			}
		}
		else {
			s = 0;
			//never to be here.!!!
		}
		return s;
	}
	
	public static GetRequest build(short ns, byte[] pkey, byte[] skey) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (skey != null && ((pkey.length + skey.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE)) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		GetRequest request = new GetRequest(ns , pkey, skey);
		return request;
	}
	
	public static GetRequest build(short ns, List<byte[]> keys, TairOption opt) throws IllegalArgumentException {
		if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (keys == null || keys.size() == 0) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		for (byte[] key : keys) {
			if (key == null || key.length > TairConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
			}
		}
		GetRequest request = new GetRequest(ns, keys);
		return request;
	}
}
