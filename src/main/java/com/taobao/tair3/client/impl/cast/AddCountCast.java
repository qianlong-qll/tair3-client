package com.taobao.tair3.client.impl.cast;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.IncDecResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;

public class AddCountCast implements TairResultCast<IncDecResponse, Result<Integer>> {
	public Result<Integer> cast(IncDecResponse s, Object context) throws TairRpcError {
		Result<Integer> result = new Result<Integer> ();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		if (code.equals(ResultCode.OK) || code.equals(ResultCode.NOTEXISTS)) {
			result.setResult(s.getValue());
			result.setCode(ResultCode.OK);
		}
		else {
			result.setResult(s.getValue());
			result.setCode(code);
		}	
		return result;
	}
}
