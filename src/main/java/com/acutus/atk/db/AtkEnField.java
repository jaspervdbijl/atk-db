package com.acutus.atk.db;

import com.acutus.atk.entity.AtkField;
import lombok.SneakyThrows;

import javax.persistence.Column;
import javax.persistence.Id;
import java.lang.reflect.Field;
import java.sql.ResultSet;

import static com.acutus.atk.util.StringUtils.isEmpty;

public class AtkEnField<T,R> extends AtkField<T,R> {

    public AtkEnField(Class<T> type,Field field,R entity) {
        super(type, field, entity);
    }

    public String getColName() {
        Column column = getField().getAnnotation(Column.class);
        return column != null && !isEmpty(column.name()) ? column.name() : getField().getName();
    }

    public boolean isId() {
        return getField().getAnnotation(Id.class) != null;
    }

    @SneakyThrows
    public void setFromRs(ResultSet rs) {
        set(mapFromRs(rs, getType(), getColName()));
    }
}
