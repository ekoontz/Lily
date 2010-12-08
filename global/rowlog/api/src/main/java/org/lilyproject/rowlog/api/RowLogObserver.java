package org.lilyproject.rowlog.api;


public interface RowLogObserver {
    /**
     * Notifies the rowlog configuration parameters have changed.
     *
     * @param config the new rowlog configuration.
     */
    void rowLogConfigChanged(RowLogConfig config);
}
