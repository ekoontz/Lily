package org.lilycms.rowlog.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilycms.rowlog.api.RowLogMessageListener;

public class ListenerClassMapping {
    private Log log = LogFactory.getLog(getClass());

    public static ListenerClassMapping INSTANCE = new ListenerClassMapping();
    
    private Map<String, String> mapping = Collections.synchronizedMap(new HashMap<String, String>());

    private ListenerClassMapping() {
    }
    
    public void put(String id, String className) {
        mapping.put(id, className);
    }
    
    public String get(String id) {
        return mapping.get(id);
    }
    
    public RowLogMessageListener getListener(String id) {
        String className = get(id);
        if (className == null)
            return null;
        try {
            return (RowLogMessageListener)Class.forName(className).newInstance();
        } catch (Exception e) {
            log.warn("RowLogMessageListener with name <"+className+"> could not be instantiated", e);
            return null;
        }
    }
    
    public void remove(String id) {
        mapping.remove(id);
    }
}
