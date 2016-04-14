package com.taobao.tair3.client.packets.dataserver;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.packets.AbstractResponsePacket;

public class SimplePrefixGetMultiResponse extends AbstractResponsePacket {

	protected int configVersion;
	 protected List<Result<byte[]>> resultEntries = new ArrayList<Result<byte[]>>();

	 public List<Result<byte[]>> getResult() {
		return resultEntries;
	 }
	public boolean hasConfigVersion() {
		return true;
	}
	
	private class Slice {
    	public short  len;
    	public byte[] buf;
    }
    
    private Slice readSlice(ChannelBuffer byteBuffer) {
    	Slice s = new Slice();
    	s.len = byteBuffer.readShort();
    	if (s.len > 0) {
    		s.buf = new byte[s.len];
    		byteBuffer.readBytes(s.buf);
    	}
    	return s;
    }

	@Override
	public void decodeFrom(ChannelBuffer buff) {
		resultCode = buff.readShort();
		short kvcount = buff.readShort();
		for (int i = 0; i < kvcount; ++i) {
			int primecode = buff.readShort();
			Slice primeKey = readSlice(buff);
			Slice primeVal = readSlice(buff);
			// Object primeKeyObj = transcoder.decode(primeKey.buf, 2,
			// primeKey.len - 2);

			if (primeVal.len != 0) {
				// skip two area bytes
				// Object primeValObj = transcoder.decode(primeVal.buf, 2,
				// primeVal.len - 2);
				// DataEntry da = new DataEntry(primeKeyObj, primeValObj, 0, 0);
				// resultEntries.add(new
				// Result<DataEntry>(ResultCode.valueOf(primecode), da));
				int valSize = primeVal.len - 2;
				byte[] val = null;
				if (valSize <= 0) {

				} else {
					val = new byte[valSize];
					System.arraycopy(primeVal.buf, 2, val, 0, valSize);
					Result<byte[]> r = new Result<byte[]>();
					r.setKey(primeKey.buf);
					r.setResult(val);
					r.setCode(ResultCode.castResultCode(primecode));
					resultEntries.add(r);
				}
			}

			short subcount = buff.readShort();
			while (subcount-- > 0) {
				int subcode = buff.readShort();
				Slice subKey = readSlice(buff);
				Slice subVal = readSlice(buff);
				// Object subKeyObj = transcoder.decode(subKey.buf);
				// Object subValObj = subVal.len == 0 ?
				// null :
				// transcoder.decode(subVal.buf, 2, subVal.len - 2);
				// DataEntry da = new DataEntry(subKeyObj, subValObj, 0, 0);
				Result<byte[]> r = new Result<byte[]>();
				if (subVal.len > 2) {
					r.setKey(subKey.buf);
					byte[] subV = new byte[subVal.len - 2];
					System.arraycopy(subVal.buf, 2, subV, 0, subVal.len - 2);
					r.setResult(subV);
					r.setCode(ResultCode.castResultCode(subcode));
					resultEntries.add(r);
				}
			}
		}
	}
	@Override
	public int decodeResultCodeFrom(ChannelBuffer bb) {
		int r = (int)bb.getShort(4);
		return r;
	}

}
