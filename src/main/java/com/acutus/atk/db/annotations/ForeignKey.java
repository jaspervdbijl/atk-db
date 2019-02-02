package com.acutus.atk.db.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.acutus.atk.db.annotations.ForeignKey.Deferrability.NotDeferrable;

/*
 * Annotate Entity Fields with @ForeignKey to specify whether the field is a entity foreign key to a referring table
 * @param Class refers to the Entity class (referred table)
 * @param Field. Optional. Refers to the field in the entity. Default to the Entity'd id field
 * @param onUpdateAction. Optional. If the parent table is updated specify if the appropriate action. NoAction (default). Set Null, Set default value or Cascade
 * @param onUpdateDelete. Optional. If the parent table is deleted specify if the appropriate action. NoAction (default). Set Null, Set default value or Cascade
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ForeignKey {

    Class table();

    String field();

    String name();

    Action onDeleteAction() default Action.SetNull;

    Deferrability deferrable() default NotDeferrable;

    public enum Action {
        NoAction(3), SetDefault(4), SetNull(2), Cascade(0), Restrict(1);

        int code;

        Action(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    public enum Deferrability {

        InitiallyDeferred(5), InitiallyImmediate(6), NotDeferrable(7);

        int code;

        Deferrability(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

}
