package com.acutus.atk.db;

import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.db.util.AtkEnUtil;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.call.CallThree;
import com.acutus.atk.util.collection.Tuple4;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.GeneratedValue;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.acutus.atk.db.sql.SQLHelper.*;
import static com.acutus.atk.db.util.PersistHelper.preProcessInsert;
import static com.acutus.atk.db.util.PersistHelper.preProcessUpdate;
import static com.acutus.atk.util.AtkUtil.handle;
import static javax.persistence.GenerationType.AUTO;
import static javax.persistence.GenerationType.IDENTITY;

@Slf4j
public class Persist<T extends AbstractAtkEntity> {

    private static Optional<CallThree<Connection, AbstractAtkEntity, Boolean>> PERSIST_CALLBACK = Optional.empty();

    public static void setPersistCallback(CallThree<Connection, AbstractAtkEntity, Boolean> callback) {
        PERSIST_CALLBACK = Optional.of(callback);
    }

    private T entity;

    public Persist(T entity) {
        this.entity = entity;
    }

    private boolean populateIdAndReturnIsAutoIncrement(AtkEnField id) {
        GeneratedValue generatedValue = id.getField().getAnnotation(GeneratedValue.class);
        if (generatedValue != null && (generatedValue.strategy().equals(AUTO) || generatedValue.strategy().equals(IDENTITY))) {
            if (String.class.equals(id.getType()) && id.get() == null) {
                id.set(UUID.randomUUID().toString());
            }
            return (Number.class.isAssignableFrom(id.getType()));
        }
        return false;
    }

    protected Tuple4<AtkEnFields, AtkEnFields, Boolean, String> prepareInsert() {
        preProcessInsert(entity);
        // no id
        boolean autoInc = false;
        AtkEnFields ids = entity.getEnFields().getIds();
        if (ids.size() == 1) {
            autoInc = populateIdAndReturnIsAutoIncrement(ids.get(0));
        }
        // exclude id is its a auto generated value
        AtkEnFields clone = entity.getEnFields().clone().removeWhen(c -> c.get() == null);
        if (autoInc) {
            clone.removeAll(ids);
        }
        String sql = String.format("insert into %s (%s) values(%s)", entity.getTableName(), clone.getColNames().toString(","),
                clone.stream().map(f -> "?").reduce((s1, s2) -> s1 + "," + s2).get());

        return new Tuple4(ids, clone, autoInc, sql);
    }

    @SneakyThrows
    public T batchInsert(PreparedStatement batchPS) {
        Tuple4<AtkEnFields, AtkEnFields, Boolean, String> prepared = prepareInsert();
        AtkEnFields clone = prepared.getSecond();
        prepare(batchPS, wrapForPreparedStatement(clone).toArray());
        batchPS.addBatch();
        return entity;
    }

    @SneakyThrows
    public T insert(Connection connection) {
        Tuple4<AtkEnFields, AtkEnFields, Boolean, String> prepared = prepareInsert();
        AtkEnFields ids = prepared.getFirst();
        AtkEnFields clone = prepared.getSecond();
        boolean autoInc = prepared.getThird();
        String sql = prepared.getFourth();

        try (PreparedStatement ps = prepare(connection, sql, wrapForPreparedStatement(clone).toArray())) {
            ps.executeUpdate();
        }
        // load any auto inc fields
        if (autoInc) {
            ids.get(0).set(DriverFactory.getDriver(connection).getLastInsertValue(connection, ids.get(0).getType()));
        }
        PERSIST_CALLBACK.ifPresent(c -> handle(() -> c.call(connection, entity, true)));
        entity.setLoadedFromDB(true);
        return entity;
    }

    private List wrapForPreparedStatement(AtkEnFields fields) {
        // note that a lambda stream will remove null values
        List values = new ArrayList();
        for (AtkEnField field : fields) {
            values.add(AtkEnUtil.wrapForPreparedStatement(field));
        }
        return values;
    }

    public T insert(DataSource dataSource) {
        return runAndReturn(dataSource, c -> insert(c));
    }

    private void assertIdIsPresent(AtkEnFields ids) {
        Assert.isTrue(!ids.isEmpty(), "No Id fields defined for entity " + entity.getTableName());
        Assert.isTrue(!ids.stream().filter(f -> f.get() == null).findAny().isPresent()
                , "Ids can not be null %s %s", entity.getTableName(), entity.getEnFields().getIds());
    }

    /**
     * update on the entity id
     *
     * @param connection
     * @return
     */
    @SneakyThrows
    private T update(Connection connection, AtkEnFields updateFields) {
        long t1 = System.currentTimeMillis();
        String sql = "";
        try {
            List<Optional<AtkEnField>> mod = preProcessUpdate(entity);
            updateFields.addAll(mod.stream().filter(o -> o.isPresent()).map(o -> o.get()).collect(Collectors.toList()));
            AtkEnFields ids = entity.getEnFields().getIds();
            assertIdIsPresent(ids);
            // remove the ids
            updateFields = updateFields.removeWhen(f -> ids.contains(f));
            AtkEnFields updateValues = updateFields.clone();

            if (updateFields.isEmpty()) {
                return entity;
            }
            // add the ids to the end
            updateValues.addAll(ids);

            sql = String.format("update %s set %s where %s",
                    entity.getTableName(), updateFields.getColNames().append("= ?").toString(","),
                    entity.getEnFields().getIds().getColNames().append("= ?").toString(","));

            try (PreparedStatement ps = prepare(connection, sql, wrapForPreparedStatement(updateValues).toArray(new Object[]{}))) {
                int updated = ps.executeUpdate();

                Assert.isTrue(updated == 1, "Failed to update %s on %s", entity.getTableName(), entity.getEnFields().getIds());

                PERSIST_CALLBACK.ifPresent(c -> handle(() -> c.call(connection, entity, false)));

                entity.getEnFields().reset();
            }
            return entity;
        } catch (Exception ex) {
            log.error(sql);
            throw ex;
        } finally {
            long s2 = System.currentTimeMillis();
            if (s2 - t1 > 1000) {
                log.debug("Update SLow " + ((s2 - t1) / 1000) + " " + sql);
            }
        }
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
     * delete on the entity id
     *
     * @param connection
     * @return
     */
    @SneakyThrows
    public void deleteOnId(Connection connection) {
        assertIdIsPresent(entity.getEnFields().getIds());
        try (PreparedStatement ps = prepare(connection,
                String.format("delete from %s where %s"
                        , entity.getTableName()
                        , entity.getEnFields().getIds().getColNames().append("= ?").toString(","))
                , entity.getEnFields().getIds().getValues().toArray())) {
            int updated = ps.executeUpdate();
            Assert.isTrue(updated == 1, "Failed to delete %s on %s", entity.getTableName(), entity.getEnFields().getIds());
        }
    }

    /**
     * delete on the entity set fields
     *
     * @param connection
     * @return
     */
    @SneakyThrows
    public void deleteOnSet(Connection connection) {
        Assert.notEmpty(entity.getEnFields().getSet(), "Atleast one fields must be set");
        StringBuilder where = new StringBuilder();
        entity.getEnFields().getSet().stream().forEach(field -> where.append(field.getColName()).append(" = ?").append(" and "));

        try (PreparedStatement ps = prepare(connection,
                String.format("delete from %s where %s"
                        , entity.getTableName()
                        , where.substring(0, where.lastIndexOf(" and ")))
                , entity.getEnFields().getSet().getValues().toArray())) {

            ps.executeUpdate();
        }
    }

    public void deleteOnSet(DataSource dataSource) {
        run(dataSource, c -> deleteOnSet(c));
    }

    public void deleteOnId(DataSource dataSource) {
        run(dataSource, c -> deleteOnId(c));
    }

}
