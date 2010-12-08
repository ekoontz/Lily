package org.lilyproject.rowlog.api;

public class RowLogConfig {

    private long lockTimeout;
    private boolean respectOrder;
    private boolean enableNotify;
    private long notifyDelay;
    private long minimalProcessDelay;

    /**
     * A value object bundling the configuration paramaters for a rowlog and its processors.
     * @see RowLogConfigurationManager
     * 
     * @param lockTimeout the timeout a message will be locked for a certain subscription
     * @param respsectOrder true if the order of subscriptions needs to be respected for the rowlog
     * @param enableNotify true if the processor need to be notified of new messages being put on the rowlog
     * @param notifyDelay the minimal delay between two notify messages to be sent to the processor
     * @param minimalProcessDelay the minimal age a messages needs to have before a processor will pick it up for processing
     */
    public RowLogConfig(long lockTimeout, boolean respsectOrder, boolean enableNotify, long notifyDelay, long minimalProcessDelay) {
        this.lockTimeout = lockTimeout;
        this.respectOrder = respsectOrder;
        this.enableNotify = enableNotify;
        this.notifyDelay = notifyDelay;
        this.minimalProcessDelay = minimalProcessDelay;
    }

    public long getLockTimeout() {
        return lockTimeout;
    }
    
    public void setLockTimeout(long lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    public boolean isRespectOrder() {
        return respectOrder;
    }
    
    public void setRespectOrder(boolean respectOrder) {
        this.respectOrder = respectOrder;
    }
    
    public boolean isEnableNotify() {
        return enableNotify;
    }

    public void setEnableNotify(boolean enableNotify) {
        this.enableNotify = enableNotify;
    }

    public long getNotifyDelay() {
        return notifyDelay;
    }

    public void setNotifyDelay(long notifyDelay) {
        this.notifyDelay = notifyDelay;
    }

    public long getMinimalProcessDelay() {
        return minimalProcessDelay;
    }

    public void setMinimalProcessDelay(long minimalProcessDelay) {
        this.minimalProcessDelay = minimalProcessDelay;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (enableNotify ? 1231 : 1237);
        result = prime * result + (int) (lockTimeout ^ (lockTimeout >>> 32));
        result = prime * result + (int) (minimalProcessDelay ^ (minimalProcessDelay >>> 32));
        result = prime * result + (int) (notifyDelay ^ (notifyDelay >>> 32));
        result = prime * result + (respectOrder ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RowLogConfig other = (RowLogConfig) obj;
        if (enableNotify != other.enableNotify)
            return false;
        if (lockTimeout != other.lockTimeout)
            return false;
        if (minimalProcessDelay != other.minimalProcessDelay)
            return false;
        if (notifyDelay != other.notifyDelay)
            return false;
        if (respectOrder != other.respectOrder)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "RowLogConfig [lockTimeout=" + lockTimeout + ", respectOrder=" + respectOrder + ", enableNotify="
                + enableNotify + ", notifyDelay=" + notifyDelay + ", minimalProcessDelay=" + minimalProcessDelay + "]";
    }
}
