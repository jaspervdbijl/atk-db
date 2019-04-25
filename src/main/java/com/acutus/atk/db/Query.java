package com.acutus.atk.db;

import com.acutus.atk.db.sql.Filter;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.call.CallOne;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.acutus.atk.db.sql.Filter.Type.AND;
import static com.acutus.atk.db.sql.Filter.and;
import static com.acutus.atk.db.sql.SQLHelper.*;
import static com.acutus.atk.util.AtkUtil.handle;

public class Query<T extends AbstractAtkEntity> {

    private T entity;

    public Query(T entity) {
        this.entity = entity;
    }

    @SneakyThrows
    private PreparedStatement prepareStatementFromFilter(Connection connection, Filter filter) {
        return filter.prepare(connection.prepareStatement(
                String.format("select %s from %s where %s"
                        , entity.getEnFields().getColNames().toString(",")
                        , entity.getTableName(), filter.getSql())));
    }

    @SneakyThrows
    public Optional<T> get(Connection connection, Filter filter) {
        try (PreparedStatement ps = prepareStatementFromFilter(connection, filter)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of((T) entity.set(rs));
                }
            }
        }
        return Optional.empty();
    }

    public Optional<T> get(DataSource dataSource, Filter filter) {
        return runAndReturn(dataSource, c -> get(c, filter));
    }


    @SneakyThrows
    public void iterate(Connection connection, Filter filter, CallOne<T> call) {
        try (PreparedStatement ps = prepareStatementFromFilter(connection, filter)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    call.call((T) entity.set(rs));
                }
            }
        }
    }

    public void iterate(DataSource dataSource, Filter filter, CallOne<T> call) {
        run(dataSource, c -> iterate(c, filter, call));
    }

    public List<T> getAll(Connection connection, Filter filter) {
        List<T> list = new ArrayList<>();
        iterate(connection, filter, e -> list.add(e));
        return list;
    }

    public static <T> T populateFrom(ResultSet rs, T t) {
        Reflect.getFields(t.getClass()).stream()
                .forEach(f -> handle(() -> f.set(t, mapFromRs(rs, f.getType(), f.getName()))));
        return t;
    }

    public List<T> getAll(DataSource dataSource, Filter filter) {
        return runAndReturn(dataSource, c -> getAll(c, filter));
    }

    public List<T> getAll(DataSource dataSource) {
        return runAndReturn(dataSource, c -> getAll(c, new Filter()));
    }

    public List<T> getAllBySet(DataSource dataSource) {
        return runAndReturn(dataSource, c -> getAll(c, new Filter(AND, entity.getEnFields().getSet())));
    }

    public Optional<T> getBySet(Connection connection) {
        return getBySet(connection, entity.getEnFields().getSet());
    }

    @SneakyThrows
    private Optional<T> getBySet(Connection connection, AtkEnFields set) {
        Assert.isTrue(!set.isEmpty(), "No set fields for entity %s ", entity.getTableName());
        return get(connection, and(set.toArray(new AtkEnField[]{})));
    }

    public Optional<T> getBySet(DataSource dataSource) {
        return runAndReturn(dataSource, c -> getBySet(c, entity.getEnFields().getSet()));
    }

    public Optional<T> findById(DataSource dataSource) {
        return runAndReturn(dataSource, c -> findById(c));
    }

    /**
     * find a single entity
     *
     * @return
     */
    @SneakyThrows
    public Optional<T> findById(Connection connection) {
        AtkEnFields ids = entity.getEnFields().getIds();
        Assert.isTrue(!ids.isEmpty(), "No Primary keys defined for entity %s ", entity.getTableName());
        Assert.isTrue(ids.getSet().size() == ids.size(), "Null id values. entity %s ", entity.getTableName());
        return getBySet(connection, ids);

    }

}
