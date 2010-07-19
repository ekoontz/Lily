package org.lilycms.tools.import_;

public interface ImportListener {
    void conflict(EntityType entityType, String entityName, String propName, Object oldValue, Object newValue)
            throws ImportConflictException;

    void existsAndEqual(EntityType entityType, String entityName, String entityId);

    void updated(EntityType entityType, String entityName, String entityId, long version);

    void created(EntityType entityType, String entityName, String entityId);    
}
