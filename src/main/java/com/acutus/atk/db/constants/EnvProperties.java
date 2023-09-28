package com.acutus.atk.db.constants;

import com.acutus.atk.property.PropertyField;

public class EnvProperties {

    // ## DB Forward engineering properties

    // if strict mode option is enabled, this any column size changes will also be altered
    public static PropertyField<Boolean> DB_FE_STRICT = new PropertyField<>("atk.db.forward_engineering.strict", true);

    public static PropertyField<Boolean> DB_FE_ALLOW_DROP = new PropertyField<>("atk.db.forward_engineering.allow.drop", false);

}
