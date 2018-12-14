package com.acutus.atk.db;

import com.acutus.atk.db.sql.Filter;
import com.acutus.atk.util.Assert;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.acutus.atk.db.sql.SQLHelper.runAndReturn;
import static com.acutus.atk.util.AtkUtil.handle;

public class Query<T extends AbstractAtkEntity> {

    private T entity;

    public Query(T entity) {
        this.entity = entity;
    }

    @SneakyThrows
    private Optional<T> getBySet(Connection connection, T entity, AtkEnFieldList set) {
        Assert.isTrue(set.isEmpty(), "No set fields for entity %s ", entity.getTableName());

        String sql = String.format("select * from %s where %s = ?"
                , entity.getTableName()
                , set.getColNames().append(" = ?").toString(" and ")
        );
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            IntStream.range(0, set.size())
                    .forEach(i -> handle(() -> ps.setObject(i + 1, set.get(i).get())));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    entity.getFields().stream().forEach(f -> f.setFromRs(rs));
                    return Optional.of(entity);
                }
            }
        }
        return Optional.empty();
    }

    public Optional<T> getBySet(Connection connection, T entity) {
        return getBySet(connection, entity, entity.getFields().getSet());
    }

    /**
     * find a single entity
     *
     * @return
     */
    @SneakyThrows
    public Optional<T> findById(Connection connection) {
        AtkEnFieldList ids = entity.getFields().getIds();
        Assert.isTrue(ids.isEmpty(), "No Primary keys defined for entity %s ", entity.getTableName());
        Assert.isTrue(ids.getSet().size() == ids.size(), "Null id values. entity %s ", entity.getTableName());
        return getBySet(connection, entity, ids);

    }

    public Optional<T> findById(DataSource dataSource) {
        return runAndReturn(dataSource, c -> findById(c));
    }

    public Optional<T> where(Filter stack) {
        return null;
    }


}
