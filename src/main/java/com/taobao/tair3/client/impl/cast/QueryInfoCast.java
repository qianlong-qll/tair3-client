package com.taobao.tair3.client.impl.cast;

import java.util.Map;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.configserver.QueryInfoResponse;
public class QueryInfoCast implements TairResultCast<QueryInfoResponse,Result<Map<String, String>>> {

	public Result<Map<String, String>> cast(QueryInfoResponse s, Object context)
			throws TairRpcError, TairCastIllegalContext {
		Result<Map<String, String>> result = new Result<Map<String, String>>();
		result.setCode(ResultCode.OK);
		result.setResult(s.getInfoMap());
		return result;
	}


}
