package com.taobao.tair3.client.impl.cast;
import java.util.List;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.RangeResponse;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;

public class GetRangeKeyCast implements TairResultCast<RangeResponse, Result<List<Result<byte[]>>>> {

	public Result<List<Result<byte[]>>> cast(RangeResponse s, Object context)
			throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof byte[])) {
		throw new  TairCastIllegalContext("context of GetRangeKeyCast.");
	}
	Result<List<Result<byte[]>>> result = new Result<List<Result<byte[]>>>();
	List<Result<byte[]>> list = s.getResults();
	ResultCode code = ResultCode.castResultCode(s.getCode());
	byte[] key = (byte[]) context;
	result.setCode(code);
	result.setKey(key);
	result.setResult(list);
	result.setFlag(s.getFlag());
	return result;
	}
//	public Result<List<Result<byte[]>>> cast(RangeResponse s, Object context) throws TairRpcError, TairCastIllegalContext {

//	}	
}
