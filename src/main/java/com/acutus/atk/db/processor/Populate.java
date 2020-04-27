package com.acutus.atk.db.processor;

import com.acutus.atk.entity.processor.Atk;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Populate {

    // Resource to json data to populate the table with
    String value();

}
