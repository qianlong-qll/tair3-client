package com.taobao.tair3.client.impl.cast;

import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.dataserver.BatchPutResponse;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;

public class BatchPutCast implements TairResultCast<BatchPutResponse, Result<ResultMap<byte[], Result<Void>>>> {

	public Result<ResultMap<byte[], Result<Void>>> cast(BatchPutResponse s,
			Object context) throws TairRpcError, TairCastIllegalContext {
		return null;
	}
	
}
