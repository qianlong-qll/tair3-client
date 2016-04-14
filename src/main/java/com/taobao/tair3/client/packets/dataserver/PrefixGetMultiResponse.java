package com.taobao.tair3.client.packets.dataserver;
import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.packets.AbstractResponsePacket;


public class PrefixGetMultiResponse extends AbstractResponsePacket {
    protected int configVersion = 0;
    protected byte[] pkey = null;
    protected ResultMap<byte[], Result<byte[]>> datas = null;
    protected int successCount = 0;
    protected int failedCount = 0;

    public int getConfigVersion() {
    	return configVersion;
    }

    public byte[] getPKey() {
    	return pkey;
    }
    
    public ResultMap<byte[], Result<byte[]>> getResults() {
    	return datas;
    }

    public boolean hasConfigVersion() {
    	return true;
    }
    @Override
    public void decodeFrom(ChannelBuffer buff) {
    	resultCode = buff.readInt();
    	
    	decodeMeta(buff);
    	int size = buff.readInt();
    	
    	size -= PREFIX_KEY_TYPE.length;
    	byte[] pkey = new byte[size];
    	buff.skipBytes(PREFIX_KEY_TYPE.length);
    	buff.readBytes(pkey);

    	successCount = buff.readInt();
		if (successCount > 0) {
			datas = new ResultMap<byte[], Result<byte[]>>(successCount);
			for (int i = 0; i < successCount; ++i) {
				Result<byte[]> r = new Result<byte[]>();
				decodeMeta(buff, r);
				size = buff.readInt();
				if (size > 0) {
					byte[] key = new byte[size];
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
				int rc = buff.readInt();
				ResultCode code = ResultCode.castResultCode(rc);
				r.setCode(code);
				if (r.getKey() != null) {
					datas.put(r.getKey(), r);
				}
			}
		}
    	failedCount = buff.readInt();
		if (failedCount > 0) {
			if (datas == null) {
				datas = new ResultMap<byte[], Result<byte[]>>(failedCount);
			}
			for (int i = 0; i < failedCount; ++i) {
				Result<byte[]> r = new Result<byte[]>();
				decodeMeta(buff, r);
				size = buff.readInt();
				byte[] key = null;
				if (size > 0) {
					key = new byte[size];
					buff.readBytes(key);
					r.setKey(key);
					r.setResult(null);
				}
				int rc = buff.readInt();
				ResultCode code = ResultCode.castResultCode(rc);
				r.setCode(code);
				if (key != null) {
					datas.put(key, r);
				}
			}
		}
		if (datas != null) {
			datas.setKey(pkey);
		}
    }
}