package com.acutus.atk.db.util;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.annotations.Default;
import com.acutus.atk.db.annotations.UID;
import com.acutus.atk.db.annotations.audit.CreatedBy;
import com.acutus.atk.db.annotations.audit.CreatedDate;
import com.acutus.atk.db.annotations.audit.LastModifiedBy;
import com.acutus.atk.db.annotations.audit.LastModifiedDate;
import com.acutus.atk.entity.AtkField;
import com.acutus.atk.io.IOUtil;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.call.CallOne;
import com.acutus.atk.util.call.CallOneRet;
import lombok.SneakyThrows;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.swing.text.html.Option;
import java.lang.annotation.Annotation;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

import static com.acutus.atk.beans.BeanHelper.decode;
import static com.acutus.atk.db.util.AtkEnUtil.unwrapEnumerated;
import static com.acutus.atk.db.util.UserContextHolder.getUsername;
import static com.acutus.atk.util.AtkUtil.handle;

public class PersistHelper {
    private static Map<String,String> defaultResourceMap = new HashMap<>();
    private static Map<Class<? extends Annotation>, CallOneRet<AtkEnField,Optional<AtkEnField>>> insertPreProcessor = new HashMap<>();
    private static Map<Class<? extends Annotation>, CallOneRet<AtkEnField,Optional<AtkEnField>>> updatePreProcessor = new HashMap<>();

    static {
        insertPreProcessor.put(CreatedDate.class, (e) -> processCreatedDate(e));
        insertPreProcessor.put(CreatedBy.class, (e) -> processCreatedBy(e));
        insertPreProcessor.put(UID.class, (e) -> processUID(e));
        insertPreProcessor.put(Default.class, (e) -> processDefault(e, true));
    }

    static {
        updatePreProcessor.put(LastModifiedDate.class, (e) -> processCreatedDate(e));
        updatePreProcessor.put(LastModifiedBy.class, (e) -> processCreatedBy(e));
    }

    @SneakyThrows
    public static List<Optional<AtkEnField>> preProcess(AbstractAtkEntity entity,Map<Class<? extends Annotation>, CallOneRet<AtkEnField,Optional<AtkEnField>>> processor) {
        List<Optional<AtkEnField>> fields = new ArrayList<>();
        entity.getEnFields().stream().filter(f -> f.get() == null).forEach(field -> {
            for (Annotation a : field.getField().getAnnotations()) {
                if (processor.containsKey(a.annotationType())) {
                    fields.add(handle(() -> processor.get(a.annotationType()).call(field)));
                }
            }
        });
        return fields;
    }

    @SneakyThrows
    public static List<Optional<AtkEnField>> preProcessInsert(AbstractAtkEntity entity) {
        return preProcess(entity,insertPreProcessor);
    }

    @SneakyThrows
    public static List<Optional<AtkEnField>> preProcessUpdate(AbstractAtkEntity entity) {
        return preProcess(entity,updatePreProcessor);
    }


    @SneakyThrows
    public static Optional<AtkEnField> processCreatedDate(AtkEnField field) {
        boolean wasNull = field.get() == null;
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
        return wasNull && field.get() != null ? Optional.of(field) : Optional.empty();
    }

    public static Optional<AtkEnField> processCreatedBy(AtkEnField field) {
        boolean wasNull = field.get() == null;
        field.set(field.get() == null ? getUsername() : field.get());
        return wasNull ? Optional.of(field) : Optional.empty();
    }

    public static Optional<AtkEnField> processUID(AtkEnField field) {
        Assert.isTrue(field.getType().equals(String.class), "UID may only be applied to Strings. Field " + field);
        field.set(UUID.randomUUID().toString());
        return Optional.of(field);
    }

    @SneakyThrows
    private static String getDefaultResource(String keyName) {
        if (!defaultResourceMap.containsKey(keyName)) {
            defaultResourceMap.put(keyName,IOUtil.readAvailableAsStr(Thread.currentThread().getContextClassLoader().getResourceAsStream(keyName)));
        }
        return defaultResourceMap.get(keyName);
    }

    @SneakyThrows
    public static Optional<AtkEnField> processDefault(AtkEnField field, boolean insert) {
        Default def = field.getField().getAnnotation(Default.class);
        if (insert == (def.type() == Default.Type.INSERT || def.type() == Default.Type.BOTH)
                || !insert == (def.type() == Default.Type.UPDATE || def.type() == Default.Type.BOTH)) {
            Enumerated enumerated = (Enumerated) field.getField().getAnnotation(Enumerated.class);
            Class type = enumerated != null
                    ? enumerated.value().equals(EnumType.STRING)
                    ? String.class : Integer.class
                    : field.getType();
            String value = def.value();
            value = value.startsWith("res://") ? getDefaultResource(value) : value;
            field.set(unwrapEnumerated(field.getField(), decode(type, value)));
            return Optional.of(field);
        }
        return Optional.empty();
    }


}
