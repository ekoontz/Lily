package org.lilyproject.testclientfw;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * This file was copied from Mule: Copyright (c) MuleSource, Inc. All rights reserved. http://www.mulesource.com
 *
 * A handler for unexecutable tasks that waits until the task can be submitted for
 * execution or times out. Generously snipped from the jsr166 repository at: <a
 * HREF="http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/java/util/concurrent/ThreadPoolExecutor.java"></a>.
 */
public class WaitPolicy implements RejectedExecutionHandler {
    private final long _time;
    private final TimeUnit _timeUnit;

    /**
     * Constructs a <tt>WaitPolicy</tt> which waits (almost) forever.
     */
    public WaitPolicy() {
        // effectively waits forever
        this(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    /**
     * Constructs a <tt>WaitPolicy</tt> with timeout. A negative <code>time</code>
     * value is interpreted as <code>Long.MAX_VALUE</code>.
     */
    public WaitPolicy(long time, TimeUnit timeUnit) {
        super();
        _time = (time < 0 ? Long.MAX_VALUE : time);
        _timeUnit = timeUnit;
    }

    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
        try {
            if (e.isShutdown() || !e.getQueue().offer(r, _time, _timeUnit)) {
                throw new RejectedExecutionException();
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException(ie);
        }
    }

}