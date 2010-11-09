package org.lilyproject.rowlog.api;

public interface ProcessorNotifyObserver {
	/**
	 * Method to be called when a message has been posted on the rowlog that needs to be processed.
	 */
	void notifyProcessor();
}
