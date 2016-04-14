package com.taobao.tair3.client.impl.cast;
import java.util.List;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.PrefixIncDecResponse;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.Pair;


public class PrefixAddCountBoundedMultiCast implements TairResultCast<PrefixIncDecResponse, Result<ResultMap<byte[], Result<Integer>>>> {
	public Result<ResultMap<byte[], Result<Integer>>> cast(PrefixIncDecResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		 if (context == null || !(context instanceof Pair<?, ?>)) {
			throw new  TairCastIllegalContext("context of PrefixAddCountMultiCast.");
		}
	 
		@SuppressWarnings("unchecked")
		Pair<byte[], List<ByteArray>> pair = (Pair<byte[], List<ByteArray>>) context;
		byte[] pkey = pair.first();

		
		Result<ResultMap<byte[], Result<Integer>>> result = new Result<ResultMap<byte[], Result<Integer>>>();
		ResultMap<byte[], Result<Integer>> resultMap = s.getResults();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		result.setCode(code);
		if (resultMap == null) {
			resultMap = new ResultMap<byte[], Result<Integer>>(0);
		}
		resultMap.setCode(code);
		resultMap.setKey(pkey);
		result.setResult(resultMap);
		return result;
	}
}
