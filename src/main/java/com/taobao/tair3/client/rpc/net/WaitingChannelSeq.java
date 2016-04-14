package com.taobao.tair3.client.rpc.net;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class WaitingChannelSeq implements Delayed {
	public TairChannel session;
	public Integer chid;
	private long delayed;
	
	public WaitingChannelSeq(TairChannel session, Integer chid, long timeout) {
		this.session = session;
		this.chid = chid;
		delayed = timeout + System.currentTimeMillis();
	}

	@Override
	public int compareTo(Delayed o) {
		WaitingChannelSeq obj = (WaitingChannelSeq)o;
		long r = this.delayed - obj.delayed;
		if (r < 0) 
			return -1;
		else if (r == 0)
			return 0;
		else
			return 1;
	}

	@Override
	public long getDelay(TimeUnit unit) {
		return unit.convert(delayed - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
		//return delayed - System.currentTimeMillis();
	}
}
