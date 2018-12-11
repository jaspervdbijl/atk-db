package com.acutus.atk.db;

import com.acutus.atk.db.processor.AtkEntity;
import com.acutus.atk.entity.AbstractAtk;
import com.acutus.atk.entity.AtkFieldList;

public class AbstractAtkEntity extends AbstractAtk {

    @Override
    public AtkEnFieldList getFields() {
        AtkEnFieldList fields = new AtkEnFieldList();
        fields.addAll(super.getFields());
        return fields;
    }
}
