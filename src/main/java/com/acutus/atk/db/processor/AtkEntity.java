package com.acutus.atk.db.processor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface AtkEntity {
    String className() default "";

    String classNameExt() default "Entity";

    // 0 will always upgrade
    int version() default 0;
}
