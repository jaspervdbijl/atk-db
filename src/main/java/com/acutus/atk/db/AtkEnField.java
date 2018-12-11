package com.acutus.atk.db;

import com.acutus.atk.entity.AtkField;
import com.acutus.atk.entity.processor.Atk;

import javax.persistence.Column;
import java.lang.reflect.Field;
import java.util.Collections;

import static com.acutus.atk.util.StringUtils.isEmpty;

public class AtkEnField<T,R> extends AtkField<T,R> {


    public AtkEnField(Class<T> type,Field field,R entity) {
        super(type, field, entity);
    }

    public String getColName() {
        Column column = getField().getAnnotation(Column.class);
        return column != null && isEmpty(column.name()) ?column.name():getField().getName();
    }
}
