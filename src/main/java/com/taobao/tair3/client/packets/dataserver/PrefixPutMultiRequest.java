package com.taobao.tair3.client.packets.dataserver;

import java.util.Iterator;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.Pair;
import com.taobao.tair3.client.TairClient.RequestOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairUtil;

public class PrefixPutMultiRequest extends AbstractRequestPacket {
	protected short namespace;
	protected byte[] pkey = null;
	protected Map<byte[], Pair<byte[], RequestOption>> kvs = null; // = new
																	// HashMap<byte[],
																	// Pair<byte[],
																	// RequestOption>>
																	// ();
	protected Map<byte[], Pair<byte[], RequestOption>> cvs = null; // = new
																	// HashMap<byte[],
																	// Pair<byte[],
																	// RequestOption>>
																	// ();

	public PrefixPutMultiRequest(short ns, byte[] pkey,
			Map<byte[], Pair<byte[], RequestOption>> kvs,
			Map<byte[], Pair<byte[], RequestOption>> cvs) {
		this.namespace = ns;
		this.kvs = kvs;
		this.cvs = cvs;
		this.pkey = pkey;
	}

	@Override
	public void encodeTo(ChannelBuffer out) {
		out.writeByte((byte) 0); // 1
		out.writeShort(namespace); // 2

		encodeDataMeta(out);
		out.writeInt(pkey.length + PREFIX_KEY_TYPE.length);
		out.writeBytes(PREFIX_KEY_TYPE);
		out.writeBytes(pkey);
		int kvSize = 0;
		if (kvs != null) {
			kvSize += kvs.size();
		}
		if (cvs != null) {
			kvSize += cvs.size();
		}
		out.writeInt(kvSize);
		if (kvs != null) {
			for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : kvs
					.entrySet()) {
				byte[] skey = entry.getKey();
				Pair<byte[], RequestOption> value = entry.getValue();
				if (skey == null || value == null || value.isAvaliable() == false) {
					throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
				}
				int keySize = pkey.length + PREFIX_KEY_TYPE.length;
				keySize <<= 22;
				keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);
				

				encodeDataMeta(out, value.second().getVersion(), TairUtil.getDuration(value.second().getExpire()));
				out.writeInt(keySize);
				out.writeBytes(PREFIX_KEY_TYPE);
				out.writeBytes(pkey);
				out.writeBytes(skey);

				encodeDataMeta(out);
				out.writeInt(value.first().length);
				out.writeBytes(value.first());
			}
		}
		if (cvs != null) {
			for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : cvs
					.entrySet()) {
				byte[] skey = entry.getKey();
				Pair<byte[], RequestOption> value = entry.getValue();
				if (skey == null || value == null || value.isAvaliable() == false) {
					throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
				}
				int keySize = pkey.length + PREFIX_KEY_TYPE.length;
				keySize <<= 22;
				keySize |= (pkey.length + skey.length + PREFIX_KEY_TYPE.length);

				encodeDataMeta(out, value.second().getVersion(), TairUtil.getDuration(value.second().getExpire()));
				out.writeInt(keySize);
				out.writeBytes(PREFIX_KEY_TYPE);
				out.writeBytes(pkey);
				out.writeBytes(skey);

				encodeDataMeta(out, TairConstant.TAIR_ITEM_FLAG_ADDCOUNT);
				out.writeInt(value.first().length);
				out.writeBytes(value.first());
			}
		}
	}

	public int size() {
		int s = 1 + 2 + 40 + (pkey.length + PREFIX_KEY_TYPE.length) + 4;
		if (kvs != null) {
			Iterator<Map.Entry<byte[], Pair<byte[], RequestOption>>> i = kvs
					.entrySet().iterator();
			while (i.hasNext()) {
				Map.Entry<byte[], Pair<byte[], RequestOption>> entry = i.next();
				byte[] skey = entry.getKey();
				Pair<byte[], RequestOption> value = entry.getValue();
				s += (40 + (pkey.length + skey.length + PREFIX_KEY_TYPE.length));
				s += (40 + value.first().length);
			}
		}
		if (cvs != null) {
			Iterator<Map.Entry<byte[], Pair<byte[], RequestOption>>> i = cvs
					.entrySet().iterator();
			while (i.hasNext()) {
				Map.Entry<byte[], Pair<byte[], RequestOption>> entry = i.next();
				byte[] skey = entry.getKey();
				Pair<byte[], RequestOption> value = entry.getValue();
				s += (40 + (pkey.length + skey.length + PREFIX_KEY_TYPE.length));
				s += (40 + value.first().length);
			}
		}
		return s;
	}

	public static PrefixPutMultiRequest build(short ns, byte[] pkey,
			Map<byte[], Pair<byte[], RequestOption>> keyValuePairs,
			Map<byte[], Pair<byte[], RequestOption>> keyCounterPairs) {
		if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
			throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
		}
		if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
			throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
		}
		if (keyValuePairs == null && keyCounterPairs == null) {
			throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
		}
		if (keyValuePairs != null) {
			if (keyValuePairs.size() == 0) {
				throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
			}
			for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : keyValuePairs
					.entrySet()) {
				byte[] skey = entry.getKey();
				if (skey == null
						|| (pkey.length + skey.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
					throw new IllegalArgumentException(
							TairConstant.KEY_NOT_AVAILABLE);
				}
				if (entry.getValue() == null) {
					throw new IllegalArgumentException(
							TairConstant.VALUE_NOT_AVAILABLE);
				}
				if (entry.getValue().first() == null
						|| entry.getValue().second() == null) {
					throw new IllegalArgumentException(
							TairConstant.VALUE_NOT_AVAILABLE);
				}
			}
		}
		if (keyCounterPairs != null) {
			if (keyCounterPairs.size() == 0) {
				throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
			}
			for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : keyCounterPairs
					.entrySet()) {
				byte[] skey = entry.getKey();
				if (skey == null
						|| (pkey.length + skey.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
					throw new IllegalArgumentException(
							TairConstant.KEY_NOT_AVAILABLE);
				}
				if (entry.getValue() == null) {
					throw new IllegalArgumentException(
							TairConstant.VALUE_NOT_AVAILABLE);
				}
				if (entry.getValue().first() == null
						|| entry.getValue().second() == null) {
					throw new IllegalArgumentException(
							TairConstant.VALUE_NOT_AVAILABLE);
				}
			}
		}
		PrefixPutMultiRequest request = new PrefixPutMultiRequest(ns, pkey,
				keyValuePairs, keyCounterPairs);
		return request;
	}
	public short getNamespace() {
		return this.namespace;
	}
}
