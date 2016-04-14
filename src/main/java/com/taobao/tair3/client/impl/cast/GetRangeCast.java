package com.taobao.tair3.client.impl.cast;
import java.util.List;

import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.RangeResponse;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.Pair;


public class GetRangeCast implements TairResultCast<RangeResponse, Result<List<Pair<byte[], Result<byte[]>>>>> {
	public Result<List<Pair<byte[], Result<byte[]>>>> cast(RangeResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof byte[])) {
			throw new  TairCastIllegalContext("context of GetRangeCast.");
		}
		Result<List<Pair<byte[], Result<byte[]>>>> result = new Result<List<Pair<byte[], Result<byte[]>>>>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		result.setCode(code);
		short type = s.getType();
		List<Pair<byte[], Result<byte[]>>> r = s.getOrderedResults();
		if ((type == TairConstant.RANGE_ALL || type == TairConstant.RANGE_ALL_REVERSE) && r != null) {
			result.setCode(code);
			result.setResult(r);
			
		}
		result.setCode(code);
		result.setResult(r);
		result.setFlag(s.getFlag());
		return result;
	}
}
