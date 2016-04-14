package com.taobao.tair3.client.impl.cast;

import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.common.ReturnResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;

public class PutCast implements TairResultCast<ReturnResponse, Result<Void>> {

	public Result<Void> cast(ReturnResponse s, Object context) throws TairRpcError {
		Result<Void> result = new Result<Void>();
		result.setCode(ResultCode.castResultCode(s.getCode()));
		return result;
	}
}
