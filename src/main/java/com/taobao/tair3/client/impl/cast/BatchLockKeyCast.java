package com.taobao.tair3.client.impl.cast;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;

public class BatchLockKeyCast implements TairResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof byte[])) {
			throw new  TairCastIllegalContext("context of BatchLockKey.");
		}
		byte[] key = (byte[]) context;
		Result<ResultMap<byte[], Result<Void>>> res = new Result<ResultMap<byte[], Result<Void>>>();
		ResultMap<byte[], Result<Void>> resMap = new ResultMap<byte[], Result<Void>>();
		Result<Void> rr = new Result<Void>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		rr.setCode(code);
		rr.setKey(key);
		resMap.setCode(code);
		resMap.put(key, rr);
		res.setResult(resMap);
		res.setCode(code);
		return res;
	}
}
