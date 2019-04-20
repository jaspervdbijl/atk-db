package com.acutus.atk.db;

import com.acutus.atk.db.annotations.Index;
import com.acutus.atk.util.Strings;

public class AtkEnIndex {

    private String fieldName;
    private AbstractAtkEntity entity;

    public AtkEnIndex(String fieldName, AbstractAtkEntity entity) {
        this.fieldName = fieldName;
        this.entity = entity;
        entity.addIndex(this);
    }

    public Index getIndex() {
        return entity.getRefFields().get(fieldName).get().getAnnotation(Index.class);
    }

    public String getName() {
        return getIndex().name();
    }

    public boolean isUnique() {
        return getIndex().unique();
    }

    public Strings getColumns() {
        return Strings.asList(getIndex().columns());
    }

    public AtkEnFields getFields() {
        AtkEnFields fields = new AtkEnFields();
        for (String col : getColumns()) {
            fields.add(entity.getEnFields().getByColName(col).get());
        }
        return fields;
    }
}
