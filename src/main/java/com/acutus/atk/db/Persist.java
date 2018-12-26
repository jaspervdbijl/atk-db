package com.acutus.atk.db;

public class Persist<T extends AbstractAtkEntity> {

    private T entity;

    public Persist(T entity) {
        this.entity = entity;
    }

    public T insert() {
        // no id
        return entity;
    }
}
