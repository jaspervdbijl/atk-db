package com.acutus.atk.db;

import com.acutus.atk.entity.AbstractAtk;
import com.acutus.atk.util.StringUtils;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Table;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AbstractAtkEntity<T, O> extends AbstractAtk<T, O> {

    @Getter
    @Setter
    private transient boolean isLoadedFromDB;

    @Getter
    private transient List<AtkEnIndex> indexes = new ArrayList<>();

    // entities created as child classes will have this reference set to the parent
    @Getter
    private transient AbstractAtkEntity parentEntity;

    protected void addIndex(AtkEnIndex index) {
        indexes.add(index);
    }

    public String getTableName() {
        Table table = getClass().getAnnotation(Table.class);
        return table != null && !StringUtils.isEmpty(table.name()) ? table.name() : getClass().getSimpleName();
    }

    public int version() {
        return 0;
    }

    public AtkEnFields getEnFields() {
        return new AtkEnFields(getFields());
    }

    public T set(ResultSet rs) {
        isLoadedFromDB = true;
        getEnFields().stream().forEach(f -> f.setFromRs(rs));
        return (T) this;
    }

}
