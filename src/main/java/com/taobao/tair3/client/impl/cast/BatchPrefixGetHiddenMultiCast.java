package com.taobao.tair3.client.impl.cast;
import java.util.Map;

import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.PrefixGetMultiResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.ResultMap;

public class BatchPrefixGetHiddenMultiCast implements TairResultCast<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>> {
	public Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>> cast(PrefixGetMultiResponse s, Object context) throws TairRpcError {
		//if (context == null || !(context instanceof byte[])) {
		//	throw new  IllegalArgumentException("context of BatchPrefixGetHiddenMultiCast.");
		//}
		Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>> result = new Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>();
		ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>> e = new ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>(1);
		ResultCode code = ResultCode.castResultCode(s.getCode());
		ResultMap<byte[], Result<byte[]>> datas = s.getResults();
		Result<Map<byte[], Result<byte[]>>> r = new Result<Map<byte[], Result<byte[]>>>();
		
		result.setCode(code);
		r.setCode(code);
		r.setKey(datas.getKey());
		r.setResult(datas.getResult());
		e.put(r.getKey(), r);
		e.setCode(code);
		result.setResult(e);
		return result;

	}

}
