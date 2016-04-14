package com.taobao.tair3.client.impl.cast;

import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;

public class BatchPutOldCast implements TairResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws TairRpcError {
		if (context == null || !(context instanceof byte[])) {
			throw new  IllegalArgumentException("context of BatchPutOldCast.");
		}
		byte [] key = (byte[]) context;
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>>();
		ResultMap<byte[], Result<Void>> r = new ResultMap<byte[], Result<Void>>();
		Result<Void> rr = new Result<Void>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		rr.setCode(code);
		rr.setKey(key);
		r.put(key, rr);
		r.setCode(code);
		result.setResult(r);
		result.setCode(code);
		return result;
	}
}