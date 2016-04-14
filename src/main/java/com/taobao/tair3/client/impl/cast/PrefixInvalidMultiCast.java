package com.taobao.tair3.client.impl.cast;


import java.util.List;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.Pair;

public class PrefixInvalidMultiCast implements TairResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof Pair<?,?>)) {
			throw new  TairCastIllegalContext("context of PrefixInvalidMultiCast.");
		}
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>> ();
		ResultMap<byte[], Result<Void>> resMap = new ResultMap<byte[], Result<Void>> ();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		@SuppressWarnings("unchecked")
		Pair<byte[], List<byte[]>> pair = (Pair<byte[], List<byte[]>>) context;
		byte[] pkey = pair.first();
		List<byte[]> keys = pair.second();
		for (byte[] key : keys) {
			Result<Void> r = new Result<Void>();
			r.setKey(key);
			r.setCode(code);
			resMap.put(key, r);
		}
		resMap.setKey(pkey);
		resMap.setCode(code);
		result.setCode(code);
		result.setResult(resMap);
		return result;
	}	

}
