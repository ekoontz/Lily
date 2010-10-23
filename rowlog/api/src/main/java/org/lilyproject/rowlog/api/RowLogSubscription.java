package org.lilyproject.rowlog.api;

import org.lilyproject.util.ObjectUtils;


public class RowLogSubscription implements Comparable<RowLogSubscription> {
    private final String rowLogId;
    private final String id;
    private final Type type;
    private final int maxTries;
    private final int orderNr;

    public enum Type {VM, Netty}
    
    public RowLogSubscription(String rowLogId, String id, Type type, int maxTries, int orderNr) {
        this.rowLogId = rowLogId;
        this.id = id;
        this.type = type;
        this.maxTries = maxTries;
        this.orderNr = orderNr;
    }

    public String getRowLogId() {
        return rowLogId;
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
        result = prime * result + ((rowLogId == null) ? 0 : rowLogId.hashCode());
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
        RowLogSubscription other = (RowLogSubscription) obj;
        if (!ObjectUtils.safeEquals(rowLogId, other.rowLogId))
            return false;
        if (!ObjectUtils.safeEquals(id, other.id))
            return false;
        if (maxTries != other.maxTries)
            return false;
        if (orderNr != other.orderNr)
            return false;
        if (!ObjectUtils.safeEquals(type, other.type))
            return false;
        return true;
    }

    public int compareTo(RowLogSubscription other) {
        return orderNr - other.orderNr;
    }
}
