package com.acutus.atk.db;

import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.util.Assert;
import lombok.SneakyThrows;

import javax.persistence.GeneratedValue;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;

import static com.acutus.atk.db.sql.SQLHelper.*;
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
    public T insert(Connection connection) {
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
        try (PreparedStatement ps = prepare(connection,
                String.format("insert into %s (%s) values(%s)"
                        , entity.getTableName(), clone.getColNames().toString(",")
                        , clone.stream().map(f -> "?").reduce((s1, s2) -> s1 + "," + s2).get())
                , clone.getValues().toArray())) {
            ps.executeUpdate();
        }
        // load any auto inc fields
        if (autoInc) {
            ids.get(0).set(DriverFactory.getDriver(connection)
                    .getLastInsertValue(connection, ids.get(0).getType()));
        }
        return entity;
    }

    public T insert(DataSource dataSource) {
        return runAndReturn(dataSource, c -> insert(c));
    }

    private void assertIdIsPresent() {
        Assert.isTrue(!entity.getEnFields().getIds().isEmpty(), "No Id fields defined for entity " + entity.getTableName());
        Assert.isTrue(!entity.getEnFields().getIds().getValues()
                        .stream().filter(c -> c == null).findAny().isPresent()
                , "Ids can not be null %s %s", entity.getTableName(), entity.getEnFields().getIds());

    }
    /**
     * update on the entity id
     *
     * @param connection
     * @return
     */
    @SneakyThrows
    private T update(Connection connection, AtkEnFieldList updateFields) {
        assertIdIsPresent();
        List updateValues = updateFields.getValues();
        updateFields.addAll(entity.getEnFields().getIds().getValues());
        try (PreparedStatement ps = prepare(connection,
                String.format("update %s set %s where %s"
                        , entity.getTableName(), updateFields.getColNames().append("= ?").toString(",")
                        , entity.getEnFields().getIds().getColNames().append("= ?").toString(","))
                , updateValues.toArray())) {
            int updated = ps.executeUpdate();
            Assert.isTrue(updated == 1, "Failed to update %s on %s", entity.getTableName(), entity.getEnFields().getIds());
            entity.getEnFields().reset();
        }
        return entity;
    }

    /**
     * update only set fields
     *
     * @param connection
     * @return
     */
    public T update(Connection connection) {
        return update(connection, entity.getEnFields().getSet());
    }

    /**
     * update only set fields
     *
     * @param connection
     * @return
     */
    public T updateAllColumns(Connection connection) {
        return update(connection, entity.getEnFields());
    }

    public T update(DataSource dataSource) {
        return runAndReturn(dataSource, c -> update(c));
    }

    public T updateAllColumns(DataSource dataSource) {
        return runAndReturn(dataSource, c -> updateAllColumns(c));
    }

    /**
     * update or insert entity
     *
     * @param connection
     * @return
     */
    public T set(Connection connection) {
        if (entity.isLoadedFromDB()) {
            return update(connection);
        } else {
            return insert(connection);
        }
    }

    public T set(DataSource dataSource) {
        return runAndReturn(dataSource, c -> set(c));
    }

    /**
     * update on the entity id
     *
     * @param connection
     * @return
     */
    @SneakyThrows
    public void delete(Connection connection) {
        assertIdIsPresent();
        try (PreparedStatement ps = prepare(connection,
                String.format("delete from %s where %s"
                        , entity.getTableName()
                        , entity.getEnFields().getIds().getColNames().append("= ?").toString(","))
                , entity.getEnFields().getIds().getValues().toArray())) {
            int updated = ps.executeUpdate();
            Assert.isTrue(updated == 1, "Failed to delete %s on %s", entity.getTableName(), entity.getEnFields().getIds());
        }
    }

    public void delete(DataSource dataSource) {
        run(dataSource, c -> delete(c));
    }

}