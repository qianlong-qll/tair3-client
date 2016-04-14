package com.taobao.tair3.client;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tair3.client.util.TairConstant;

public class Result<T> {
	public static class ResultCode {
		public static ResultCode ASYNC_RESULT_QUEUE_FULL = new ResultCode(-14, "async result queue is full");
		public static ResultCode UNKNOWN = new ResultCode(-1, "unknown");
		public static ResultCode OK = new ResultCode(0, "OK");
		public static ResultCode TIMEOUT = new ResultCode(-3989, "timeout");
		public static ResultCode ASYNC_INVOKE_FAILED = new ResultCode(-10, "async invoke failed");
		public static ResultCode ASYNC_SEND_SUCCESS = new ResultCode(-12, "async invoke ok");
		public static ResultCode SHOULD_PROXY = new ResultCode(-3967, "should proxy");
		public static ResultCode QUEUE_OVERLOWED = new ResultCode(-3968, "queue overlowed");
		public static ResultCode HIDDEN = new ResultCode(-3969, "hidden");
		public static ResultCode INVAL_CONN_ERROR = new ResultCode(-3970, "inval conn error");
		public static ResultCode LOCK_NOT_EXIST = new ResultCode(-3974,"lock not exist");
		public static ResultCode LOCK_ALREADY_EXIST = new ResultCode(-3975,"lock already exist");
		public static ResultCode MTIME_EARLY = new ResultCode(-3976, "mtime early");
		public static ResultCode PROXYED_ERROR = new ResultCode(-3977, "proxyed error");
		public static ResultCode DEC_NOT_FOUND = new ResultCode(-3978, "dec not found");
		public static ResultCode DEC_ZERO = new ResultCode(-3979, "dec zero");
		public static ResultCode DEC_BOUNDS = new ResultCode(-3980, "dec bounds");
		public static ResultCode CANNOT_OVERRIDE = new ResultCode(-3981,"cann't override");
		public static ResultCode INVALID_ARGUMENT = new ResultCode(-3982, "invalid argument");
		public static ResultCode PART_OK = new ResultCode(-3983, "a quasi success");
		public static ResultCode MIGRATE_BUSY = new ResultCode(-3984, "migrate busy");
		public static ResultCode WRITE_NOT_ON_MASTER = new ResultCode(-3986, "write not on master");
		public static ResultCode SERVER_CAN_NOT_WORK = new ResultCode(-3987, "server can not work");
		public static ResultCode ITEM_EMPTY = new ResultCode(-3993, "item empty");
		public static ResultCode VERSION_ERROR = new ResultCode(-3997, "version error");
		public static ResultCode NOTEXISTS = new ResultCode(-3998, "data not exist");
		public static ResultCode FAILED = new ResultCode(-3999, "failed");
		public static ResultCode PROXYED = new ResultCode(-4000, "proxyed");
		public static ResultCode REMOVE_NOT_ON_MASTER = new ResultCode(-4101, "remove not on master");
		public static ResultCode REMOVE_ONE_FAILED = new ResultCode(-4102, "remove one failed");
		public static ResultCode NONE_DATASERVER = new ResultCode(-5113, "none dataserver");
		public static ResultCode COUNTER_OUT_OF_RANGE = new ResultCode(-5114, "counter out of range");
		public static ResultCode INIT = new ResultCode(-50000, "init");
		public static ResultCode RPC_OVERFLOW = new ResultCode(-6000, "rpc overflow");
		private static Map<Integer, ResultCode> resultCodeMap = new HashMap<Integer, ResultCode>();
		static {
			ResultCode.regist(ResultCode.UNKNOWN);
			ResultCode.regist(ResultCode.OK);
			ResultCode.regist(ResultCode.ASYNC_INVOKE_FAILED);
			ResultCode.regist(ResultCode.ASYNC_SEND_SUCCESS);
			ResultCode.regist(ResultCode.CANNOT_OVERRIDE);
			ResultCode.regist(ResultCode.DEC_BOUNDS);
			ResultCode.regist(ResultCode.DEC_NOT_FOUND);
			ResultCode.regist(ResultCode.DEC_ZERO);
			ResultCode.regist(ResultCode.FAILED);
			ResultCode.regist(ResultCode.HIDDEN);
			ResultCode.regist(ResultCode.INIT);	
			ResultCode.regist(ResultCode.INVAL_CONN_ERROR);
			ResultCode.regist(ResultCode.INVALID_ARGUMENT);
			ResultCode.regist(ResultCode.ITEM_EMPTY);
			ResultCode.regist(ResultCode.LOCK_ALREADY_EXIST);
			ResultCode.regist(ResultCode.LOCK_NOT_EXIST);
			ResultCode.regist(ResultCode.MIGRATE_BUSY);
			ResultCode.regist(ResultCode.MTIME_EARLY);
			ResultCode.regist(ResultCode.NONE_DATASERVER);
			ResultCode.regist(ResultCode.NOTEXISTS);
			ResultCode.regist(ResultCode.PART_OK);
			ResultCode.regist(ResultCode.PROXYED);
			ResultCode.regist(ResultCode.PROXYED_ERROR);
			ResultCode.regist(ResultCode.QUEUE_OVERLOWED);
			ResultCode.regist(ResultCode.REMOVE_NOT_ON_MASTER);
			ResultCode.regist(ResultCode.REMOVE_ONE_FAILED);
			ResultCode.regist(ResultCode.SERVER_CAN_NOT_WORK);
			ResultCode.regist(ResultCode.SHOULD_PROXY);
			ResultCode.regist(ResultCode.VERSION_ERROR);
			ResultCode.regist(ResultCode.WRITE_NOT_ON_MASTER);
			ResultCode.regist(ResultCode.ASYNC_RESULT_QUEUE_FULL);
			ResultCode.regist(ResultCode.TIMEOUT);
			ResultCode.regist(ResultCode.COUNTER_OUT_OF_RANGE);
			ResultCode.regist(ResultCode.RPC_OVERFLOW);
		}
		private static void regist(ResultCode rc) {
			resultCodeMap.put(rc.errno(), rc);
		}
		private int code;
		private String msg;

		private ResultCode(int code, String msg) {
			this.code = code;
			this.msg = msg;
		}

		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("[(Code:" + code + ") (Message:" + msg + ")]");
			return sb.toString();
		}

		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			else if (obj instanceof ResultCode) {
				return ((ResultCode) obj).errno() == this.code;
			}
			else {
				return false;
			}
		}

		public int hashCode() {
			return this.code + 5000;
		}

		public static ResultCode castResultCode(int code) {
			if (code == -3988) {
				code = -3998;
			}
			ResultCode rc = resultCodeMap.get(code);
			if (rc == null) {
				return ResultCode.UNKNOWN;
			}
			else {
				return rc;
			}
		}

		public int errno() {
			return code;
		}
	}

	private ResultCode code;
	private T result;
	private byte[] key = null;
	private short prefixSize = 0;
	private short version;
	private int expire;
	private int modifyTime;
	private int createTime;
	private int flag = 0;

	public Result() {
		super();
		this.code = ResultCode.UNKNOWN;
	}

	public Result(ResultCode code) {
		this();
		this.code = code;
	}

	public short getVersion() {
		return version;
	}

	public void setVersion(short version) {
		this.version = version;
	}

	public int getExpire() {
		return expire;
	}

	public void setExpire(int expire) {
		this.expire = expire;
	}

	public int getModifyTime() {
		return this.modifyTime;
	}
	public void setModifyTime(int modify) {
		this.modifyTime = modify;
	}
	public void setCreateTime(int createTime) {
		this.createTime = createTime;
	}
	public int getCreateTime() {
		return createTime;
	}
	public void setCode(ResultCode code) {
		this.code = code;
	}
	public void setKey(byte[] key, short prefixSize) {
		this.key = key;
		this.prefixSize = prefixSize;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}

	public byte[] getKey() {
		return this.key;
	}

	public short getPrefixSize() {
		return prefixSize;
	}

	public ResultCode getCode() {
		return code;
	}

	public void setResult(T t) {
		this.result = t;
	}

	public T getResult() {
		return result;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	public int getFlag() {
		return flag;
	}

	public boolean isSuccess() {
		return code.errno() == 0 || code.errno() == -3998 || code.errno() == -3988;
	}

	public boolean isLocked() {
		return (this.flag  & TairConstant.TAIR_ITEM_FLAG_LOCKED) != 0;
	}
	public boolean hasNext() {
		return (this.flag  & TairConstant.TAIR_ITEM_FLAG_HAS_NEXT) != 0;
	}
	
	public boolean isCounter() {
		return (this.flag & TairConstant.TAIR_ITEM_FLAG_ADDCOUNT) != 0;
	}
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("" + code + " Result:");
		if (result instanceof byte[]) {
			String str = new String((byte[]) result);
			sb.append(str);
		} else {
			sb.append(result);
		}
		return sb.toString();
	}
}
