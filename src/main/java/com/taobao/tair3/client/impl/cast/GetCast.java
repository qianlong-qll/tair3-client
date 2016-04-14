package com.taobao.tair3.client.impl.cast;
import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.GetResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;

public class GetCast implements TairResultCast<GetResponse,Result<byte[]>> {

	public Result<byte[]> cast(GetResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof Short)) {
			throw new  TairCastIllegalContext("context of GetCast.");
		}
		Result<byte[]> result = null;
		ResultCode code = ResultCode.castResultCode(s.getCode());
		
		if (code.equals(ResultCode.OK) && s.getEntrires().size() > 0) {
			result = s.getEntrires().get(0);
		}
		else {
			result = new Result<byte[]> ();
		}
		result.setCode(code);
		return result;
	}
}
