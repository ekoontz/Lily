package org.lilycms.rowlog.api;

public class SubscriptionContext {
    private final int id;
    private final Type type;
    private final int workerCount;

    public enum Type{Embeded, Local, Remote}
    
    public SubscriptionContext(int id, Type type, int workerCount) {
        this.id = id;
        this.type = type;
        this.workerCount = workerCount;
    }
    
    public int getId() {
        return id;
    }
    
    public Type getType() {
        return type;
    }

    public int getWorkerCount() {
        return workerCount;
    }
}
