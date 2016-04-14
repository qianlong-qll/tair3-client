package com.taobao.tair3.client.packets.dataserver;
import org.jboss.netty.buffer.ChannelBuffer;
import com.taobao.tair3.client.packets.AbstractResponsePacket;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;


public class PrefixIncDecResponse  extends AbstractResponsePacket {
	private int configVersion ;
	private int successCount = 0;
	private int failedCount = 0;
	private ResultMap<byte[], Result<Integer>> datas = null;
	public int getSuccessCount() {
		return successCount;
	}
	public int getFailedCount() {
		return failedCount;
	}
	public ResultMap<byte[], Result<Integer>> getResults() {
		return datas;
	}

	public boolean hasConfigVersion() {
		return true;
	}

	@Override
	public void decodeFrom(ChannelBuffer buffer) {
		this.resultCode = buffer.readInt();
		
		int size = 0;
		this.successCount = buffer.readInt();
		
		if (this.successCount > 0) {
			datas = new ResultMap<byte[], Result<Integer>> (this.successCount);
			for (int i = 0; i < successCount; ++i) {
				decodeMeta(buffer);
				size = buffer.readInt();
				if (size > 0) {
					byte[] key = new byte[size];
					buffer.readBytes(key);
					int value = buffer.readInt();
					Result<Integer> r = new Result<Integer>();
					r.setCode(ResultCode.OK);
					r.setResult(value);
					r.setKey(key);
					datas.put(key, r);
				}
			}
		}
		
		this.failedCount = buffer.readInt();
		if (failedCount > 0) {
			if (datas == null) {
				datas = new ResultMap<byte[], Result<Integer>> (this.failedCount);
			}
			for (int i = 0; i < this.failedCount; ++i) {
				decodeMeta(buffer);
				size = buffer.readInt();
				if (size > 0) {
					byte[] key = new byte[size];
					buffer.readBytes(key);
					int rc = buffer.readInt();
					Result<Integer> r = new Result<Integer>();
					r.setCode(ResultCode.castResultCode(rc));
					r.setResult(null);
					datas.put(key, r);
				}	
			}
		}	
	}
	public int getConfigVersion() {
		return configVersion;
	}
}
