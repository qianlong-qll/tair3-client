package com.taobao.tair3.client.packets.dataserver;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.Pair;
import com.taobao.tair3.client.packets.AbstractResponsePacket;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairUtil;

public class RangeResponse extends AbstractResponsePacket {
	protected int configVersion;
	protected List<Pair<byte[], Result<byte[]>>> kvDatas = null;
	protected List<Result<byte[]>> datas = null;
	protected short flag;
	protected short type;

	public short getFlag() {
		return flag;
	}
	public short getType() {
		return type;
	}
	public List<Result<byte[]>> getResults() {
		return this.datas;
	}
	public List<Pair<byte[], Result<byte[]>>> getOrderedResults() {
		return kvDatas;
	}

	public boolean hasConfigVersion() {
		return true;
	}

	@Override
	public void decodeFrom(ChannelBuffer buff) {
		resultCode = buff.readInt();
		type = buff.readShort();
		int count = buff.readInt();
		flag = buff.readShort();
		int size = 0;
		
		if (type == TairConstant.RANGE_ALL || type == TairConstant.RANGE_ALL_REVERSE) {
			count /= 2;
			kvDatas = new ArrayList<Pair<byte[], Result<byte[]>>>();
		//	kvDatas = new ResultMap<byte[], Result<byte[]>>(new TreeMap<byte[], Result<byte[]>>(TairUtil.BYTES_COMPARATOR));
			ResultCode code = ResultCode.castResultCode(resultCode);
		//	kvDatas.setCode(code);
			for (int i = 0; i < count; ++i) {
				Result<byte[]> r = new Result<byte[]> ();
				r.setCode(code);
				decodeMeta(buff, r);
				int msize = buff.readInt();
				size = (msize & 0x3FFFFF);
				byte[] key = null;
				if (size > 0) {
					key = new byte[size];
					buff.readBytes(key);
					r.setKey(key);
				}
				decodeMeta(buff);
				size = buff.readInt();
				if (size > 0) {
					byte[] value = new byte[size];
					buff.readBytes(value);
					r.setResult(value);
				}
				if (key != null) {
					kvDatas.add(new Pair<byte[], Result<byte[]>>(key, r));
				}
			}
		}
		else {
			datas = new ArrayList<Result<byte[]>> (count);
			for (int i = 0; i < count; ++i) {
				Result<byte[]> r = new Result<byte[]> ();
				if (type == TairConstant.RANGE_KEY_ONLY || type == TairConstant.RANGE_KEY_ONLY_REVERSE
						|| type == TairConstant.RANGE_DEL || type == TairConstant.RANGE_DEL_REVERSE) {
					decodeMeta(buff, r);
					int msize = buff.readInt();
					size = (msize & 0x3FFFFF);
					if (size > 0) {
						byte[] key = new byte[size];
						buff.readBytes(key);
						r.setKey(key);
					}
				}
				if (type == TairConstant.RANGE_VALUE_ONLY || type == TairConstant.RANGE_VALUE_ONLY_REVERSE) {
					decodeMeta(buff, r);
					size = buff.readInt();
					if (size > 0) {
						byte[] value = new byte[size];
						buff.readBytes(value);
						r.setResult(value);
					}
				}
				datas.add(r);
			}
		}
	}

}
