package com.acutus.atk.db;

import com.acutus.atk.entity.AbstractAtk;
import com.acutus.atk.util.StringUtils;

import javax.persistence.Table;
import java.sql.ResultSet;

public interface AbstractAtkEntity<T> extends AbstractAtk<T> {

    public default String getTableName() {
        Table table = getClass().getAnnotation(Table.class);
        return table != null && !StringUtils.isEmpty(table.name()) ? table.name() : getClass().getSimpleName();
    }

    public default AtkEnFieldList getEnFields() {
        return (AtkEnFieldList) getFields();
    }

    public default AbstractAtkEntity set(ResultSet rs) {
        getEnFields().stream().forEach(f -> f.setFromRs(rs));
        return this;
    }
}
