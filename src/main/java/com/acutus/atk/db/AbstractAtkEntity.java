package com.acutus.atk.db;

import com.acutus.atk.db.processor.AtkEntity;
import com.acutus.atk.entity.AbstractAtk;
import com.acutus.atk.entity.AtkFieldList;
import com.acutus.atk.util.StringUtils;

import javax.persistence.Table;

public class AbstractAtkEntity extends AbstractAtk {

    public String getTableName() {
        Table table = getClass().getAnnotation(Table.class);
        return table != null && !StringUtils.isEmpty(table.name()) ? table.name() : getClass().getSimpleName();
    }

    @Override
    public AtkEnFieldList getFields() {
        AtkEnFieldList fields = new AtkEnFieldList();
        fields.addAll(super.getFields());
        return fields;
    }
}
