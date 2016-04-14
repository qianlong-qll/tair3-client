package com.taobao.tair3.client.packets.dataserver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;

public class SimplePrefixGetMultiRequest extends AbstractRequestPacket {
	protected short namespace;
    protected long	reserved;
    protected Map<ByteArray, List<byte[]>> keySet = new HashMap<ByteArray, List<byte[]>>();
    public SimplePrefixGetMultiRequest(short ns) {
    	this.namespace = ns;
    	//this.reserved = reserved;
    }
    @Override
    public int size() {
    	int s = 2 + 8 + 2;
    	for (Map.Entry<ByteArray, List<byte[]>> entry : keySet.entrySet()) {
    		if (entry.getValue().size() == 0) {
    			s += entry.getKey().getBytes().length;
    		}
    		else {
    			s += entry.getKey().getBytes().length + 2;
    		}
    		s += 4;
    		for (byte[] subkey : entry.getValue()) {
    			s += 2;
    			s += subkey.length;
    		}
    	}
    	return s;
    }
    @Override
	public void encodeTo(ChannelBuffer out) {
    	out.writeLong(reserved);
    	out.writeShort(namespace);
    	out.writeShort(keySet.size());
    	for (Map.Entry<ByteArray, List<byte[]>> entry : keySet.entrySet()) {
    		ByteArray key = entry.getKey();
    		List<byte[]> subKeys = entry.getValue();
    		if (subKeys.size() == 0) {
    			out.writeShort(key.getBytes().length);
    		}
    		else {
    			out.writeShort((short) (key.getBytes().length + 2));
				short flag = TairConstant.TAIR_STYPE_MIXEDKEY;
				flag <<= 1;
				out.writeByte((byte) ((flag >> 8) & 0xFF));
				out.writeByte((byte) (flag & 0xFF));
    		}
    		out.writeBytes(key.getBytes());
    		out.writeShort((short) (subKeys.size()));
			for (byte[] subkey : subKeys) {
				// subkey
				out.writeShort((short) subkey.length);
				out.writeBytes(subkey);
			}
    	}
    }
    public static SimplePrefixGetMultiRequest build(short ns) {
    	if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
    	SimplePrefixGetMultiRequest request = new SimplePrefixGetMultiRequest(ns);
    	return request;
    }
    public void addKeys(byte[] pkey, List<byte[]> skeys) {
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		for (byte[] key : skeys) {
			if (key == null || (pkey.length + key.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
				throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
			}
		}
    	ByteArray b = new ByteArray(pkey);
    	keySet.put(b, skeys);
    }
    public short getNamespace() {
		return this.namespace;
	}
}
