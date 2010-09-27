package org.lilycms.rowlog.api;

public class SubscriptionContext {
    private final int id;
    private final Type type;

    public enum Type{VM, Netty}
    
    public SubscriptionContext(int id, Type type) {
        this.id = id;
        this.type = type;
    }
    
    public int getId() {
        return id;
    }
    
    public Type getType() {
        return type;
    }
}
