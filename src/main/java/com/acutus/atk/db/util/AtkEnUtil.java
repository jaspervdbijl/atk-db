package com.acutus.atk.db.util;

import com.acutus.atk.db.*;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.reflection.ReflectMethods;
import com.acutus.atk.util.Assert;
import lombok.SneakyThrows;

import javax.persistence.Enumerated;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;

import static javax.persistence.EnumType.ORDINAL;
import static javax.persistence.EnumType.STRING;

public class AtkEnUtil {

    @SneakyThrows
    public static Method getMethod(Class type, String name) {
        ReflectMethods methods = Reflect.getMethods(type).filter(name).filterParams();
        Assert.isTrue(methods.size() > 0, "Expected a %s method in %s ", name, type.getName());
        return methods.get(0);
    }

    @SneakyThrows
    public static Query getQuery(AbstractAtkEntity entity, Field ... selectFilter) {
        Query query = (Query) getMethod(entity.getClass(), "query").invoke(entity);
        if (selectFilter != null) {
            query.setSelectFilter(selectFilter);
        }
        return query;
    }

    public static Query getQuery(AbstractAtkEntity entity) {
        return getQuery(entity,null);
    }

    @SneakyThrows
    public static Persist getPersist(AbstractAtkEntity entity) {
        return (Persist) getMethod(entity.getClass(), "persist").invoke(entity);
    }

    @SneakyThrows
    public static <T> T unwrapEnumerated(Field field, Object value) {
        if (value != null) {
            Enumerated enumerated = field.getAnnotation(Enumerated.class);
            if (enumerated == null) {
                return (T) value;
            } else if (enumerated != null && enumerated.value().equals(STRING)) {
                    Method valueOf = Reflect.getMethods(field.getType()).get(false, "valueOf");
                return (T) valueOf.invoke(null, value);
            } else if (enumerated != null && enumerated.value().equals(ORDINAL)) {
                Enum[] values = (Enum[]) Reflect.getMethods(field.getType()).get(false, "values").invoke(null);
                for (Enum e : values) {
                    if (value.equals(e.ordinal())) {
                        return (T) e;
                    }
                }
                throw new RuntimeException("Unsupported enum type " + field.getType() + " for value " + value);
            } else {
                throw new UnsupportedOperationException("Enum Oridinal Type Not implemented");
            }
        } else {
            return null;
        }
    }

    @SneakyThrows
    public static Object wrapForPreparedStatement(AtkEnField field) {
        Enumerated enumerated = field.getField().getAnnotation(Enumerated.class);
        if (enumerated == null || field.get() == null) {
            return field.get();
        }
        if (enumerated != null && enumerated.value().equals(STRING)) {
            return field.get().toString();
        } else {
            return ((Enum)field.get()).ordinal();
        }
    }

}
