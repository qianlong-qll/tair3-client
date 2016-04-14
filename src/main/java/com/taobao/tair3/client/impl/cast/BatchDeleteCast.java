package com.taobao.tair3.client.impl.cast;

import java.util.List;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;


public class BatchDeleteCast implements TairResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof List<?>)) {
			throw new  TairCastIllegalContext("context of BatchDeleteCast.");
		}
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>>();
		ResultMap<byte[], Result<Void>> valueMap = new ResultMap<byte[], Result<Void>>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		@SuppressWarnings("unchecked")
		List<byte[]> keys = (List<byte[]>) context;
		for (byte[] key : keys) {
			Result<Void> e = new Result<Void>();
			e.setCode(code);
			valueMap.put(key, e);
		}
		valueMap.setCode(code);
		result.setCode(code);
		result.setResult(valueMap);
		return result;
	}
}
