package com.taobao.tair3.client.impl.cast;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.PrefixGetMultiResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;


public class PrefixGetMultiCast implements TairResultCast<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>> {
	public Result<ResultMap<byte[], Result<byte[]>>> cast(PrefixGetMultiResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof Integer)) {
			throw new  TairCastIllegalContext("context of PrefixGetMultiCast.");
		}
		
		Result<ResultMap<byte[], Result<byte[]>>> result = new Result<ResultMap<byte[], Result<byte[]>>>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		ResultMap<byte[], Result<byte[]>> r = s.getResults();
		if (r == null) {
			 r = new ResultMap<byte[], Result<byte[]>>(0);
		}
		r.setCode(code);
		result.setCode(code);
		result.setResult(r);
		return result;
	}

}
