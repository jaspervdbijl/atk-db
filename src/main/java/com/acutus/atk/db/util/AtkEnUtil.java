package com.acutus.atk.db.util;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.Persist;
import com.acutus.atk.db.Query;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.reflection.ReflectMethods;
import com.acutus.atk.util.Assert;
import lombok.SneakyThrows;

import java.lang.reflect.Method;

public class AtkEnUtil {

    @SneakyThrows
    public static Method getMethod(Class type, String name) {
        ReflectMethods methods = Reflect.getMethods(type).filter(name).filterParams();
        Assert.isTrue(methods.size() == 1, "Expected a %s method in %s ", name, type.getName());
        return methods.get(0);
    }

    @SneakyThrows
    public static Query getQuery(AbstractAtkEntity entity) {
        return (Query) getMethod(entity.getClass(), "query").invoke(entity);
    }

    @SneakyThrows
    public static Persist getPersist(AbstractAtkEntity entity) {
        return (Persist) getMethod(entity.getClass(), "persist").invoke(entity);
    }
}
