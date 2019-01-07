package com.acutus.atk.db;

import com.acutus.atk.entity.AbstractAtk;
import com.acutus.atk.util.StringUtils;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Table;
import java.sql.ResultSet;

public class AbstractAtkEntity<T> extends AbstractAtk<T> {

    @Getter
    @Setter
    private transient boolean isLoadedFromDB;

    // entities created as child classes will have this reference set to the parent
    private AbstractAtkEntity parentEntity;

    public String getTableName() {
        Table table = getClass().getAnnotation(Table.class);
        return table != null && !StringUtils.isEmpty(table.name()) ? table.name() : getClass().getSimpleName();
    }

    public int version() {
        return 0;
    }

    public AtkEnFieldList getEnFields() {
        return new AtkEnFieldList(getFields());
    }

    public AbstractAtkEntity set(ResultSet rs) {
        isLoadedFromDB = true;
        getEnFields().stream().forEach(f -> f.setFromRs(rs));
        return this;
    }

}
