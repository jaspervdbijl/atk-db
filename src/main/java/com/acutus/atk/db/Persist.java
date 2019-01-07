package com.acutus.atk.db;

import com.acutus.atk.db.util.DriverFactory;
import lombok.SneakyThrows;

import javax.persistence.GeneratedValue;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

import static com.acutus.atk.db.sql.SQLHelper.prepare;
import static javax.persistence.GenerationType.AUTO;
import static javax.persistence.GenerationType.IDENTITY;

public class Persist<T extends AbstractAtkEntity> {

    private T entity;

    public Persist(T entity) {
        this.entity = entity;
    }

    private boolean populateIdAndReturnIsAutoIncrement(AtkEnField id) {
        GeneratedValue generatedValue = id.getField().getAnnotation(GeneratedValue.class);
        if (generatedValue != null
                && (generatedValue.strategy().equals(AUTO) || generatedValue.strategy().equals(IDENTITY))) {
            if (String.class.equals(id.getType()) && id.get() == null) {
                id.set(UUID.randomUUID().toString());
            }
            return (Number.class.isAssignableFrom(id.getType()));
        }
        return false;
    }

    @SneakyThrows
    public T insert(Connection connecton) {
        // no id
        boolean autoInc = false;
        AtkEnFieldList ids = entity.getEnFields().getIds();
        if (ids.size() == 1) {
            autoInc = populateIdAndReturnIsAutoIncrement(ids.get(0));
        }
        // exclude id is its a auto generated value
        AtkEnFieldList clone = entity.getEnFields().clone();
        if (autoInc) {
            clone.removeAll(ids);
        }
        try (PreparedStatement ps = prepare(connecton,
                String.format("insert into %s (%s) values(%s)"
                        , entity.getTableName(), clone.getColNames().toString(",")
                        , clone.stream().map(f -> "?").reduce((s1, s2) -> s1 + "," + s2).get())
                , clone.getValues().toArray())) {
            ps.executeUpdate();
        }
        // load any auto inc fields
        if (autoInc) {
            ids.get(0).set(DriverFactory.getDriver(connecton).getLastInsertValue(connecton, ids.get(0).getType()));
        }
        return entity;
    }
}
