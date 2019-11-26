package com.acutus.atk.db.processor;

import javax.persistence.FetchType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * sql can be tested at compile time
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface Query {
    String value();
    FetchType fetchType() default FetchType.LAZY;
}
