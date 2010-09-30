package org.lilycms.rowlog.api;


public class SubscriptionContext implements Comparable<SubscriptionContext>{
    private final String id;
    private final Type type;
    private final int maxTries;
    private final int orderNr;

    public enum Type{VM, Netty}
    
    public SubscriptionContext(String id, Type type, int maxTries, int orderNr) {
        this.id = id;
        this.type = type;
        this.maxTries = maxTries;
        this.orderNr = orderNr;
    }
    
    public String getId() {
        return id;
    }
    
    public Type getType() {
        return type;
    }
    
    public int getMaxTries() {
        return maxTries;
    }

    public int getOrderNr() {
        return orderNr;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + maxTries;
        result = prime * result + orderNr;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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
        SubscriptionContext other = (SubscriptionContext) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (maxTries != other.maxTries)
            return false;
        if (orderNr != other.orderNr)
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        return true;
    }

    public int compareTo(SubscriptionContext other) {
        return orderNr - other.orderNr;
    }
}
