package com.acutus.atk.db.util;

import afu.org.checkerframework.checker.signature.qual.SourceName;
import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.annotations.Default;
import com.acutus.atk.db.annotations.UID;
import com.acutus.atk.db.annotations.audit.CreatedDate;
import com.acutus.atk.db.processor.AtkEntity;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.call.CallOne;
import lombok.SneakyThrows;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import java.lang.annotation.Annotation;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static com.acutus.atk.beans.BeanHelper.*;
import static com.acutus.atk.db.util.AtkEnUtil.unwrapEnumerated;
import static com.acutus.atk.util.AtkUtil.handle;

public class PersistHelper {
    private static Map<Class<? extends Annotation>, CallOne<AtkEnField>> insertPreProcessor = new HashMap<>();

    static {
        insertPreProcessor.put(CreatedDate.class, (e) -> processCreatedDate(e));
        insertPreProcessor.put(UID.class, (e) -> processUID(e));
        insertPreProcessor.put(Default.class, (e) -> processDefault(e, true));
    }

    @SneakyThrows
    public static void preProcessInsert(AbstractAtkEntity entity) {
        entity.getEnFields().stream().filter(f -> f.get() == null).forEach(field -> {
            for (Annotation a : field.getField().getAnnotations()) {
                if (insertPreProcessor.containsKey(a.annotationType())) {
                    handle(() -> insertPreProcessor.get(a.annotationType()).call(field));
                }
            }
        });
    }

    @SneakyThrows
    public static void processCreatedDate(AtkEnField field) {
        if (field.get() == null) {
            if (LocalDateTime.class.isAssignableFrom(field.getType())) {
                field.set(LocalDateTime.now());
            } else if (LocalTime.class.isAssignableFrom(field.getType())) {
                field.set(LocalTime.now());
            } else if (LocalDate.class.isAssignableFrom(field.getType())) {
                field.set(LocalDate.now());
            } else if (Date.class.isAssignableFrom(field.getType())) {
                field.set(field.getType().getConstructor(long.class).newInstance(System.currentTimeMillis()));
            } else {
                throw new UnsupportedOperationException("Type not implemented " + field.getType());
            }
        }
    }

    public static void processUID(AtkEnField field) {
        Assert.isTrue(field.getType().equals(String.class), "UID may only be applied to Strings. Field " + field);
        field.set(UUID.randomUUID().toString());
    }

    public static void processDefault(AtkEnField field, boolean insert) {
        Default def = field.getField().getAnnotation(Default.class);
        if (insert == (def.type() == Default.Type.INSERT || def.type() == Default.Type.BOTH)
                || !insert == (def.type() == Default.Type.UPDATE || def.type() == Default.Type.BOTH)) {
            Enumerated enumerated = (Enumerated) field.getField().getAnnotation(Enumerated.class);
            Class type = enumerated != null
                    ? enumerated.value().equals(EnumType.STRING)
                    ? String.class : Integer.class
                    : field.getType();
            field.set(unwrapEnumerated(field.getField(), decode(type, def.value())));
        }
    }


}
