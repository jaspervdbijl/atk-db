package com.acutus.atk.db;

import com.acutus.atk.entity.AtkField;
import com.acutus.atk.util.Assert;
import lombok.SneakyThrows;
import lombok.Synchronized;

import javax.persistence.Column;
import javax.persistence.Id;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.acutus.atk.util.StringUtils.isEmpty;

public class AtkEnField<T,R> extends AtkField<T,R> {

    public static final Map<Class, Optional<Method>> PARSE_METHOD = new HashMap<>();

    @Synchronized("PARSE_METHOD")
    public static Optional<Method> getParseStringMethod(Class type) {
        if (!PARSE_METHOD.containsKey(type)) {
            PARSE_METHOD.put(type,Arrays.stream(type.getMethods())
                    .filter(m -> m.getName().startsWith("parse")
                            && m.getParameterTypes().length == 1
                            && String.class.equals(m.getParameterTypes()[0]))
                    .findAny());
        }
        return PARSE_METHOD.get(type);
    }


    public AtkEnField(Class<T> type,Field field,R entity) {
        super(type, field, entity);
    }

    public String getColName() {
        Column column = getField().getAnnotation(Column.class);
        return column != null && !isEmpty(column.name()) ? column.name() : getField().getName();
    }

    public boolean isId() {
        return getField().getAnnotation(Id.class) != null;
    }

    @SneakyThrows
    public static <T> T mapFrom(Class<T> type,Object value) {
        if (value == null) return null;

        if (value.getClass().equals(type)) {
            return (T) value;
        }
        Assert.isTrue(getParseStringMethod(type).isPresent(),"Conversion type not implemented: To %s",type);
        return (T) getParseStringMethod(type).get().invoke(null,value.toString());
    }

    @SneakyThrows
    public void setFromRs(ResultSet rs) {
        set(mapFrom(getType(),rs.getObject(getColName())));
    }
}
