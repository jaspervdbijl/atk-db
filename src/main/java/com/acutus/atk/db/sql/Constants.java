package com.acutus.atk.db.sql;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static com.acutus.atk.util.AtkUtil.handle;

public class Constants {

    public static final Map<Class, Method> RS_FUNC_INT_MAP = new HashMap<Class, Method>() {{
        put(Integer.class, handle(() -> ResultSet.class.getMethod("getInt", Integer.class)));
        put(Long.class, handle(() -> ResultSet.class.getMethod("getLong", Integer.class)));
        put(String.class, handle(() -> ResultSet.class.getMethod("getString", Integer.class)));
        put(BigDecimal.class, handle(() -> ResultSet.class.getMethod("getBigDecimal", Integer.class)));
        put(Boolean.class, handle(() -> ResultSet.class.getMethod("getBoolean", Integer.class)));
        put(Byte.class, handle(() -> ResultSet.class.getMethod("getByte", Integer.class)));
        put(Byte[].class, handle(() -> ResultSet.class.getMethod("getBytes", Integer.class)));
        put(byte[].class, handle(() -> ResultSet.class.getMethod("getBytes", Integer.class)));
        put(java.sql.Date.class, handle(() -> ResultSet.class.getMethod("getDate", Integer.class)));
        put(Double.class, handle(() -> ResultSet.class.getMethod("getDouble", Integer.class)));
        put(Short.class, handle(() -> ResultSet.class.getMethod("getShort", Integer.class)));
        put(java.sql.Time.class, handle(() -> ResultSet.class.getMethod("getTime", Integer.class)));
        put(java.sql.Timestamp.class, handle(() -> ResultSet.class.getMethod("getTimestamp", Integer.class)));
        put(java.util.Date.class, handle(() -> ResultSet.class.getMethod("getTimestamp", Integer.class)));
    }};

    public static final Map<Class, Method> RS_FUNC_INT_STR = new HashMap<Class, Method>() {{
        put(Integer.class, handle(() -> ResultSet.class.getMethod("getInt", String.class)));
        put(Long.class, handle(() -> ResultSet.class.getMethod("getLong", String.class)));
        put(String.class, handle(() -> ResultSet.class.getMethod("getString", String.class)));
        put(BigDecimal.class, handle(() -> ResultSet.class.getMethod("getBigDecimal", Integer.class)));
        put(Boolean.class, handle(() -> ResultSet.class.getMethod("getBoolean", String.class)));
        put(Byte.class, handle(() -> ResultSet.class.getMethod("getByte", String.class)));
        put(Byte[].class, handle(() -> ResultSet.class.getMethod("getBytes", String.class)));
        put(byte[].class, handle(() -> ResultSet.class.getMethod("getBytes", String.class)));
        put(java.sql.Date.class, handle(() -> ResultSet.class.getMethod("getDate", String.class)));
        put(Double.class, handle(() -> ResultSet.class.getMethod("getDouble", String.class)));
        put(Short.class, handle(() -> ResultSet.class.getMethod("getShort", String.class)));
        put(java.sql.Time.class, handle(() -> ResultSet.class.getMethod("getTime", String.class)));
        put(java.sql.Timestamp.class, handle(() -> ResultSet.class.getMethod("getTimestamp", String.class)));
        put(java.util.Date.class, handle(() -> ResultSet.class.getMethod("getTimestamp", String.class)));
    }};
}
