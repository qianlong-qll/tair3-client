package com.taobao.tair3.client.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.InflaterInputStream;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.Counter;
import com.taobao.tair3.client.TairClient.Pair;
import com.taobao.tair3.client.TairClient.RequestOption;
public class TairUtil {
	
	public static SocketAddress cast2SocketAddress(String addr) {
		String[] str = addr.split(":");
		if (str.length != 2) 
			throw new IllegalArgumentException();
		return new InetSocketAddress(str[0], Integer.valueOf(str[1]));
	}
	
	public static SocketAddress cast2SocketAddress(long id) {
		StringBuffer host = new StringBuffer(30);

		host.append((id & 0xff)).append('.');
		host.append(((id >> 8) & 0xff)).append('.');
		host.append(((id >> 16) & 0xff)).append('.');
		host.append(((id >> 24) & 0xff));

		int port = (int) ((id >> 32) & 0xffff);

		return new InetSocketAddress(host.toString(), port);
	}
	
	public static String decodeString(ChannelBuffer in) {
		int len = in.readInt();
        if (len <= 1) {
            return "";
        } else {
            byte[] b = new byte[len];
            in.readBytes(b);
            return new String(b, 0, len - 1);
        }
	}
	
	public static byte[] deflate(byte[] in) {
		ByteArrayOutputStream bos = null;

		if (in != null) {
			ByteArrayInputStream bis = new ByteArrayInputStream(in);

			bos = new ByteArrayOutputStream();

			InflaterInputStream gis;

			try {
				gis = new InflaterInputStream(bis);

				byte[] buf = new byte[8192];
				int r = -1;

				while ((r = gis.read(buf)) > 0) {
					bos.write(buf, 0, r);
				}
			} catch (IOException e) {
				bos = null;
			}
		}

		return (bos == null) ? null : bos.toByteArray();
	}
	
	private static final int MURMURHASH_M = 0x5bd1e995;
	
	public static long murMurHash(ChannelBuffer buffer) {
		int len = buffer.readableBytes();
		int h = 97 ^ len;
		int index = 0;

		while (len >= 4) {
			int k = (buffer.getByte(index) & 0xff) | ((buffer.getByte(index + 1) << 8) & 0xff00)
					| ((buffer.getByte(index + 2) << 16) & 0xff0000)
					| (buffer.getByte(index + 3) << 24);

			k *= MURMURHASH_M;
			k ^= (k >>> 24);
			k *= MURMURHASH_M;
			h *= MURMURHASH_M;
			h ^= k;
			index += 4;
			len -= 4;
		}

		switch (len) {
		case 3:
			h ^= (buffer.getByte(index + 2) << 16);

		case 2:
			h ^= (buffer.getByte(index + 1) << 8);

		case 1:
			h ^= buffer.getByte(index);
			h *= MURMURHASH_M;
		}

		h ^= (h >>> 13);
		h *= MURMURHASH_M;
		h ^= (h >>> 15);
		return ((long) h & 0xffffffffL);
	}
	
	
	public static byte[] encodeCountValue(int count) {
		// Tair server cope with IncData by little-endian(dependable)
		int flag = TairConstant.TAIR_STYPE_INCDATA;
		flag <<= 1;
		byte[] b = new byte[6];
		b[1] = (byte) (flag & 0xFF);
		b[0] = (byte) ((flag >> 8) & 0xFF);
		b[2] = (byte) (count & 0xFF);
		b[3] = (byte) ((count >> 8) & 0xFF);
		b[4] = (byte) ((count >> 16) & 0xFF);
		b[5] = (byte) ((count >> 24) & 0xFF);
		return b;
	}
	public static int decodeCountValue(byte [] b) {
		int rv	 = 0;
		int bits = 0;

		for (byte i : b) {
			rv |= (((i < 0) ? (256 + i)
					: i) << bits);
			bits += 8;
		}
		return rv;
	}

	public static <T> List<byte[]> fetchRowKey(Map<byte[], Pair<T, RequestOption>> map) { 
		List<byte[]> skeyList = new ArrayList<byte[]> ();
		for (Map.Entry<byte[], Pair<T, RequestOption>> entry : map.entrySet()) {
			skeyList.add(entry.getKey());
		}
		return skeyList;
	}
	
 
	public static List<ByteArray> fetchByteArrayKey(Map<byte[], Counter> map) { 
		List<ByteArray> skeyList = new ArrayList<ByteArray> ();
		for (Map.Entry<byte[], Counter> entry : map.entrySet()) {
			skeyList.add(new ByteArray(entry.getKey()));
		}
		return skeyList;
	}

	public static int getDuration(int expiretime) {
		int now = (int)(System.currentTimeMillis() / 1000);
		if (expiretime > now) {
			expiretime -= now;
		}
		return expiretime;
	}
	public static List<byte[]> removeDuplicateKeys(List<byte[]> keys) {
		if (keys != null) {
		Set<ByteArray> keyset = new HashSet<ByteArray> ();
		for (byte[] key : keys) {
			keyset.add(new ByteArray(key));
		}
		List<byte[]> r = new ArrayList<byte[]> ();
		for (ByteArray key : keyset) {
			if (key.getBytes() != null) {
				r.add(key.getBytes());
			}
		}
		return r;
		}
		else return null;
	}
	static class BytesComparator implements Comparator<byte[]> {

		public int compare(byte[] left, byte[] right) {
			for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
	            int a = (left[i] & 0xff);
	            int b = (right[j] & 0xff);
	            if (a != b) {
	                return a - b;
	            }
	        }
	        return left.length - right.length;
		}	
		
	}
	
	public static BytesComparator BYTES_COMPARATOR = new BytesComparator();
}
