package com.taobao.tair3.client.impl.cast;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.GetResponse;
import com.taobao.tair3.client.util.TairUtil;


public class BatchGetCast implements TairResultCast<GetResponse, Result<ResultMap<byte[], Result<byte[]>>>> {
	public Result<ResultMap<byte[], Result<byte[]>>> cast(GetResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof List<?>)) {
			throw new  TairCastIllegalContext("context of BatchGetCast.");
		}
		
		Result<ResultMap<byte[], Result<byte[]>>> result = new Result<ResultMap<byte[], Result<byte[]>>>();
		ResultMap<byte[], Result<byte[]>> r = new ResultMap<byte[], Result<byte[]>>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		result.setCode(code);
		
		
		List<byte[]> keys = (List<byte[]>) context;
		Set<byte[]> keySet = new TreeSet<byte[]>(TairUtil.BYTES_COMPARATOR);
		keySet.addAll(keys);
		if ((code.equals(ResultCode.OK) || code.equals(ResultCode.PART_OK)) && s.getEntrires() != null && s.getEntrires().size() > 0) {
			for (Result<byte[]> res : s.getEntrires()) {
				res.setCode(code);
				r.put(res.getKey(), res);
				keySet.remove(res.getKey());
			}
		}
		//with out value.
		for (byte[] key : keySet) {
			Result<byte[]> e = new Result<byte[]>();
			e.setKey(key);
			e.setCode(ResultCode.NOTEXISTS);
			r.put(key, e);
		}
		r.setCode(code);
		result.setResult(r);
		return result;
	}
}
