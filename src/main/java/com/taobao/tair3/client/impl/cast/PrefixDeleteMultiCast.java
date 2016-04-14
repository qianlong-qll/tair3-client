package com.taobao.tair3.client.impl.cast;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.taobao.tair3.client.error.TairCastIllegalContext;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.impl.TairProcessor.TairResultCast;
import com.taobao.tair3.client.packets.common.BatchReturnResponse;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairUtil;
import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.Pair;


public class PrefixDeleteMultiCast implements TairResultCast<BatchReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(BatchReturnResponse s, Object context) throws TairRpcError, TairCastIllegalContext {
		if (context == null || !(context instanceof Pair<?, ?>)) {
			throw new  TairCastIllegalContext("context of PrefixDeleteMultiCast.");
		}

		Pair<byte[], List<byte[]>> pair = (Pair<byte[], List<byte[]>>) context;
		byte[] pkey = pair.first();
		List<byte[]> keys = pair.second();
		
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>> ();
		ResultMap<byte[], Result<Void>> resMap = new ResultMap<byte[], Result<Void>> ();
		Set<byte[]> keySet = new TreeSet<byte[]> (TairUtil.BYTES_COMPARATOR);
		keySet.addAll(keys);
		ResultCode code = ResultCode.castResultCode(s.getCode());
		Map<byte[], Integer>  kcmap = s.getKeyCodeMap();
		if (kcmap != null) {
			for (Map.Entry<byte[], Integer> e : kcmap.entrySet()) {
				keySet.remove(e.getKey());
				Result<Void> r = new Result<Void>();
				r.setCode(ResultCode.castResultCode(e.getValue()));
				resMap.put(e.getKey(), r);
			}
		}
		for (byte[] key : keySet) {
			Result<Void> r = new Result<Void>();
			r.setCode(ResultCode.OK);
			resMap.put(key, r);
		}
		 
		result.setCode(code);
		resMap.setCode(code);
		resMap.setKey(pkey);
		result.setResult(resMap);
		return result;
	}

}
