package com.acutus.atk.db.processor;

import com.acutus.atk.entity.processor.Atk;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface AtkEntity {

    // if no column name is defined, what naming stratergy should be used
    ColumnNamingStrategy columnNamingStrategy() default ColumnNamingStrategy.CAMEL_CASE_UNDERSCORE;

    // if no table name is defined, what naming stratergy should be used
    TableNamingStrategy tableNamingStrategy() default TableNamingStrategy.LOWER_CASE_UNDERSCORE;

    Type type() default Type.TABLE;

    String viewSqlResource() default "";

    String className() default "";

    String classNameExt() default "Entity";

    // 0 will always upgrade
    int version() default 0;

    // will auto add auditing fields
    boolean addAuditFields() default false;

    boolean auditTable() default false;

    boolean maintainEntity() default true;
    boolean maintainColumns() default true;
    String[] maintainColumnsFilter() default {};
    boolean maintainForeignKeys() default true;
    boolean maintainIndex() default true;

    Class daoClass() default Void.class;
    Atk.Match daoMatch() default Atk.Match.FULL;
    boolean daoCopyAll() default true;


    enum ColumnNamingStrategy {
        NONE, CAMEL_CASE_UNDERSCORE
    }

    enum TableNamingStrategy {
        NONE, LOWER_CASE_UNDERSCORE
    }

    enum Type {
        TABLE, VIEW
    }

}
