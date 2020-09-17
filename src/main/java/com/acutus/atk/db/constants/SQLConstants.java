package com.acutus.atk.db.constants;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import static com.acutus.atk.util.AtkUtil.handle;

public class SQLConstants {

    public static final Map<Class, Method> RS_FUNC_INT_MAP = new HashMap<Class, Method>() {{
        put(Integer.class, handle(() -> ResultSet.class.getMethod("getInt", int.class)));
        put(Long.class, handle(() -> ResultSet.class.getMethod("getLong", int.class)));
        put(String.class, handle(() -> ResultSet.class.getMethod("getString", int.class)));
        put(Character.class, handle(() -> ResultSet.class.getMethod("getString", int.class)));
        put(BigDecimal.class, handle(() -> ResultSet.class.getMethod("getBigDecimal", int.class)));
        put(Boolean.class, handle(() -> ResultSet.class.getMethod("getBoolean", int.class)));
        put(Byte.class, handle(() -> ResultSet.class.getMethod("getByte", int.class)));
        put(Byte[].class, handle(() -> ResultSet.class.getMethod("getBytes", int.class)));
        put(java.sql.Date.class, handle(() -> ResultSet.class.getMethod("getDate", int.class)));
        put(Double.class, handle(() -> ResultSet.class.getMethod("getDouble", int.class)));
        put(Short.class, handle(() -> ResultSet.class.getMethod("getShort", int.class)));
        put(java.sql.Time.class, handle(() -> ResultSet.class.getMethod("getTime", int.class)));
        put(java.sql.Timestamp.class, handle(() -> ResultSet.class.getMethod("getTimestamp", int.class)));
        put(java.util.Date.class, handle(() -> ResultSet.class.getMethod("getTimestamp", int.class)));
        put(LocalDateTime.class, handle(() -> ResultSet.class.getMethod("getTimestamp", int.class)));
        put(LocalDate.class, handle(() -> ResultSet.class.getMethod("getTimestamp", int.class)));
        put(LocalTime.class, handle(() -> ResultSet.class.getMethod("getTimestamp", int.class)));
    }};

    public static final Map<Class, Method> RS_FUNC_INT_STR = new HashMap<Class, Method>() {{
        put(Integer.class, handle(() -> ResultSet.class.getMethod("getInt", String.class)));
        put(Long.class, handle(() -> ResultSet.class.getMethod("getLong", String.class)));
        put(String.class, handle(() -> ResultSet.class.getMethod("getString", String.class)));
        put(Character.class, handle(() -> ResultSet.class.getMethod("getString", String.class)));
        put(BigDecimal.class, handle(() -> ResultSet.class.getMethod("getBigDecimal", String.class)));
        put(Boolean.class, handle(() -> ResultSet.class.getMethod("getBoolean", String.class)));
        put(Byte.class, handle(() -> ResultSet.class.getMethod("getByte", String.class)));
        put(Blob.class, handle(() -> ResultSet.class.getMethod("getBytes", String.class)));
        put(Blob.class, handle(() -> ResultSet.class.getMethod("getBytes", String.class)));
        put(Clob.class, handle(() -> ResultSet.class.getMethod("getString", String.class)));
        put(java.sql.Date.class, handle(() -> ResultSet.class.getMethod("getDate", String.class)));
        put(Double.class, handle(() -> ResultSet.class.getMethod("getDouble", String.class)));
        put(Short.class, handle(() -> ResultSet.class.getMethod("getShort", String.class)));
        put(java.sql.Time.class, handle(() -> ResultSet.class.getMethod("getTime", String.class)));
        put(java.sql.Timestamp.class, handle(() -> ResultSet.class.getMethod("getTimestamp", String.class)));
        put(LocalDateTime.class, handle(() -> ResultSet.class.getMethod("getTimestamp", String.class)));
        put(LocalDate.class, handle(() -> ResultSet.class.getMethod("getTimestamp", String.class)));
        put(LocalTime.class, handle(() -> ResultSet.class.getMethod("getTimestamp", String.class)));
        put(java.util.Date.class, handle(() -> ResultSet.class.getMethod("getTimestamp", String.class)));
    }};

    public static void init() {
    }
}
