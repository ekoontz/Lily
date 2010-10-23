package org.lilyproject.rest;

public class PostAction<T> {
    private String action;
    private T entity;

    public PostAction(String action, T entity) {
        this.action = action;
        this.entity = entity;
    }

    public String getAction() {
        return action;
    }

    public T getEntity() {
        return entity;
    }
}
