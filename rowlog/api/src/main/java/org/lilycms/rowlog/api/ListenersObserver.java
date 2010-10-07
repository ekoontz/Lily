package org.lilycms.rowlog.api;

import java.util.List;

public interface ListenersObserver {

    /**
     * Notifies the listeners have changed.
     *
     * <p>An observer will never be called from more than one thread concurrently, i.e. all
     * changes are reported sequentially.

     * @param listeners the full list of current listeners.
     */
    void listenersChanged(List<String> listeners);

}
