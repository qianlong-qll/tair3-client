package com.taobao.tair3.client.impl.invalid;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InvalidServer {
	protected static final Logger log = LoggerFactory.getLogger(InvalidServer.class);
    private int maxFailCount = 30;
    private SocketAddress address = null;
    private AtomicInteger failcount = new AtomicInteger(0);

    public InvalidServer(SocketAddress address) {
        this.address = address;
    }

    public SocketAddress getAddress() {
        return address;
    }

    public int getFailCount() {
        return failcount.get();
    }

    public int incFailCount() {
    	return failcount.incrementAndGet();
    }
    public void resetFailCount() {
        failcount.set(0);
    }

    public void hadFail() {
        if (failcount.incrementAndGet() == maxFailCount) {
            log.warn("invalid server " + this.address + " is down");
        }
    }

    public void hadSuccess() {
        int now = failcount.addAndGet(-2);
        if (now <= 0)
            failcount.set(0);
    }

    public int getMaxFailCount() {
        return maxFailCount;
    }

    public void setMaxFailCount(int maxFailCount) {
        this.maxFailCount = maxFailCount;
    }
}
