package com.acutus.atk.db.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;

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

    String name() default "";

    Action onDeleteAction() default Action.SetNull;

    Deferrability deferrable() default NotDeferrable;

    public enum Action {
        NoAction(1,3), SetDefault(4), SetNull(2), Cascade(0), Restrict(1);

        int[] codes;

        Action(int ... codes) {
            this.codes = codes;
        }

        public int[] getCode() {
            return codes;
        }

        public boolean matches(int index) {
            return Arrays.stream(codes).anyMatch(code -> code == index);
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
