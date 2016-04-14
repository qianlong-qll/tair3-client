package com.taobao.tair3.client.impl.cast;

import java.util.List;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;


public class BatchInvalidCast implements TairResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof List<?>)) {
			throw new  TairCastIllegalContext("context of BatchInvalidCast.");
		}
		@SuppressWarnings("unchecked")
		List<byte[]> keys = (List<byte[]>)context;
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>> ();
		ResultMap<byte[], Result<Void>> resMap = new ResultMap<byte[], Result<Void>> ();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		for (byte[] key : keys) {
			Result<Void> r = new Result<Void>();
			r.setCode(code);
			r.setKey(key);
			resMap.put(key, r);
		}
		result.setResult(resMap);
		resMap.setCode(code);
		result.setCode(code);
		return result;
	}

}
