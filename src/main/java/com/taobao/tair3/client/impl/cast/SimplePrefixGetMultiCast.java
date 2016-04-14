package com.taobao.tair3.client.impl.cast;
import java.util.List;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.SimplePrefixGetMultiResponse;

public class SimplePrefixGetMultiCast
		implements
		TairResultCast<SimplePrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>> {

	public Result<ResultMap<byte[], Result<byte[]>>> cast(
			SimplePrefixGetMultiResponse s, Object context)
			throws TairRpcError, TairCastIllegalContext {
		List<Result<byte[]>> res = s.getResult();
		ResultMap<byte[], Result<byte[]>> rm = new ResultMap<byte[], Result<byte[]>>(); 
		if (res != null) {
			for (Result<byte[]> r : res) {
				if (r.getKey() != null) {
					rm.put(r.getKey(), r);
				}
			}
		}
		Result<ResultMap<byte[], Result<byte[]>>> result = new Result<ResultMap<byte[], Result<byte[]>>>();
		result.setCode(ResultCode.castResultCode(s.getCode()));
		rm.setCode(ResultCode.castResultCode(s.getCode()));
		result.setResult(rm);
		return result;
	}
}
