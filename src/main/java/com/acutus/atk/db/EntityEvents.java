package com.acutus.atk.db;

import com.acutus.atk.util.call.CallOne;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.Map;

import static com.acutus.atk.util.AtkUtil.handle;

public class EntityEvents {
    private static Map<Class, CallOne>
            insertMap = new HashMap<>(),
            updateMap = new HashMap<>(),
            deleteMap = new HashMap<>(0);

    public static <T> void registerInsert(Class<T> clazz, CallOne<T> call) {
        insertMap.put(clazz, call);
    }

    public static <T> void registerUpdate(Class<T> clazz, CallOne<T> call) {
        updateMap.put(clazz, call);
    }

    public static <T> void registerDelete(Class<T> clazz, CallOne<T> call) {
        deleteMap.put(clazz, call);
    }

    public static void insertEvent(Object entity) {
        if (insertMap.containsKey(entity.getClass())) {
            handle(() -> insertMap.get(entity.getClass()).call(entity));
        }
    }

    public static void updateEvent(Object entity) {
        if (updateMap.containsKey(entity.getClass())) {
            handle(() -> updateMap.get(entity.getClass()).call(entity));
        }
    }

    @SneakyThrows
    public static void deleteEvent(Object entity) {
        if (deleteMap.containsKey(entity.getClass())) {
            handle(() -> deleteMap.get(entity.getClass()).call(entity));
        }
    }

}
