package com.taobao.tair3.client.rpc.net;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

public class DeamondThreadFactory implements ThreadFactory {
    static final AtomicInteger poolNumber = new AtomicInteger(1);
    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;
    
    static {
    	ThreadRenamingRunnable.setThreadNameDeterminer(new ThreadNameDeterminer() {

			public String determineThreadName(String currentThreadName,
					String proposedThreadName) throws Exception {
				return currentThreadName;
			}
			
		});
    }

    public DeamondThreadFactory(String prefix) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null)? s.getThreadGroup() :
                             Thread.currentThread().getThreadGroup();
        namePrefix = prefix + "-pool-" +
                      poolNumber.getAndIncrement() +
                     "-tair-netty-nio-thread-";
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r,
                              namePrefix + threadNumber.getAndIncrement(),
                              0);
        if (!t.isDaemon())
            t.setDaemon(true);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    };
}
