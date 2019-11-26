package com.acutus.atk.db;

import com.acutus.atk.db.sql.Filter;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.call.CallNilRet;
import com.acutus.atk.util.call.CallOne;
import com.acutus.atk.util.collection.Two;
import lombok.SneakyThrows;

import javax.persistence.FetchType;
import javax.persistence.OneToMany;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Stream;

import static com.acutus.atk.db.Query.OrderBy.DESC;
import static com.acutus.atk.db.driver.DriverFactory.getDriver;
import static com.acutus.atk.db.sql.Filter.Type.AND;
import static com.acutus.atk.db.sql.Filter.and;
import static com.acutus.atk.db.sql.SQLHelper.*;
import static com.acutus.atk.util.AtkUtil.getGenericFieldType;
import static com.acutus.atk.util.AtkUtil.handle;

public class Query<T extends AbstractAtkEntity,O> {

    public enum OrderBy {
        ASC, DESC
    }

    private T entity;
    private Integer limit;
    private AtkEnFields orderBy;
    private OrderBy orderByType;

    public Query(T entity) {
        this.entity = entity;
    }

    private String prepareSql(Filter filter) {
        String sql = filter.isCustom() ?
                filter.getCustomSql()
                : String.format("select %s from %s where %s %s"
                , entity.getEnFields().excludeIgnore().getColNames().toString(",")
                , entity.getTableName(), filter.getSql(),
                orderBy != null
                        ? "order by " + orderBy.getColNames().toString(",") + " " + orderByType.name()
                        : ""
        );
        return sql;
    }

    @SneakyThrows
    private PreparedStatement prepareStatementFromFilter(Connection connection, Filter filter) {
        String sql = prepareSql(filter);
        return filter.prepare(connection.prepareStatement(limit != null
                ? getDriver(connection).limit(sql, limit) : sql));
    }

    @SneakyThrows
    public Optional<T> get(Connection connection, Filter filter) {
        try (PreparedStatement ps = prepareStatementFromFilter(connection, filter)) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of((T) ((T) entity.clone()).set(rs));
                }
            }
        }
        return Optional.empty();
    }

    public Optional<T> get(Connection connection, String sql, Object... params) {
        return get(connection, new Filter(sql, params));
    }

    public Optional<T> get(DataSource dataSource, Filter filter) {
        return runAndReturn(dataSource, c -> get(c, filter));
    }

    public Optional<T> get(DataSource dataSource, String sql, Object... params) {
        return runAndReturn(dataSource, c -> get(c, sql, params));
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

    public void iterateBySet(DataSource dataSource, CallOne<T> call) {
        run(dataSource, c -> iterate(c, and(entity.getEnFields().getSet().toArray(new AtkEnField[]{})), call));
    }

    public AtkEntities<T> getAll(Connection connection, Filter filter) {
        AtkEntities<T> list = new AtkEntities<>();
        iterate(connection, filter, e -> list.add((T) e.clone()));
        return list;
    }

    private String getTmpTablename(int dept) {
        return ""+(char)(65+dept);
    }

    @SneakyThrows
    private String getLeftJoin(int cnt,AbstractAtkEntity entity,AtkEnRelation re) {
        AbstractAtkEntity e = (AbstractAtkEntity) re.getType().getConstructor().newInstance();
        AtkEnFields fk = e.getEnFields().getForeignKeys(this.entity.getClass());
        String tableName = getTmpTablename(cnt);
        Assert.isTrue(fk.size() == 1,"FK ["+fk.getColNames()+"] not located for " + entity.getTableName());
        return String.format("left join %s %s on %s = %s.%s", e.getTableName(),tableName,
                entity.getEnFields().getSingleId().getTableAndColName(),tableName,fk.get(0).getColName()) + " " +
                getLeftJoin(cnt++,e);
    }

    public static Stream<Field> getEagerFields(AbstractAtkEntity entity) {
        return entity.getRefFields().filter(f ->
                f.getAnnotation(OneToMany.class) != null
                        && f.getAnnotation(OneToMany.class).fetch()
                        .equals(FetchType.EAGER))
                .stream();
    }

    private String getLeftJoin(int cnt,AbstractAtkEntity entity) {
        Optional<String> lj = getEagerFields(entity).map(f -> handle(() -> {

            // find the entity relation field
            Optional<Field> enRefField = entity.getRefFields().get(f.getName() + "Ref");
            Assert.isTrue(enRefField.isPresent(), "Field not found " + f.getName() + "Ref");
            return getLeftJoin(cnt,entity, (AtkEnRelation) enRefField.get().get(entity));
        })).reduce((s1, s2) -> s1 + " " + s2);
        return lj.isPresent()? lj.get() : "";
    }

    private String keyToString(AbstractAtkEntity entity) {
        return entity.getTableName() + "_" + entity.getEnFields().getIds()
                .stream().map((f1 -> f1.get().toString())).reduce((o1, o2) -> o1 + "" + o2).get();
    }

    private Two<AbstractAtkEntity,Boolean> loadCascade(int cnt, Map<String, AbstractAtkEntity> map, AbstractAtkEntity entity, ResultSet rs) {
        if (cnt > -1) {
            entity.setTableName(getTmpTablename(cnt));
        }
        final int dept = cnt+1;
        entity.set(rs);
        String key = keyToString(entity);
        boolean existed = map.containsKey(key);
        if (!map.containsKey(key)) {
            map.put(key, (AbstractAtkEntity) entity.clone());
        }
        AbstractAtkEntity local = map.get(key);
        getEagerFields(local).forEach(f -> handle(() -> {
            AbstractAtkEntity child = (AbstractAtkEntity) getGenericFieldType(f).getConstructor().newInstance();
            Two<AbstractAtkEntity,Boolean> value = loadCascade(dept,map, child, rs);
            if (!value.getSecond()) {
                f.set(local,f.get(local) == null?new AtkEntities<>():f.get(local));
                ((List) f.get(local)).add(child);
            }
        }));
        return new Two<>(local,existed);
    }

    @SneakyThrows
    public AtkEntities<T> getAllCascade(Connection connection, Filter filter) {
        String sql = prepareSql(filter);
        // add all the left joins
        String lj = getLeftJoin(0,entity);
        if (sql.indexOf(" where ") != -1) {
            sql = sql.substring(0,sql.indexOf(" where ")) + " " + lj + sql.substring(sql.indexOf(" where "));
        } else {
            sql += lj;
        }
        Map<String, AbstractAtkEntity> map = new HashMap<>();
        List<String> keys = new ArrayList<>();
        AtkEntities<T> entities = new AtkEntities<>();
        try (PreparedStatement ps = prepareStatementFromFilter(connection, new Filter(sql, filter.getCustomParams()))) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Two<AbstractAtkEntity,Boolean> value = loadCascade(-1,map, entity, rs);
                    if (!value.getSecond()) {
                        entities.add((T) value.getFirst());
                    }
                }
            }
        }
        return entities;
    }

    public AtkEntities<T> getAllCascade(DataSource dataSource, Filter filter) {
        return runAndReturn(dataSource, connection -> getAllCascade(connection, filter));
    }

    public AtkEntities<T> getAll(Connection connection, String sql, Object... params) {
        return getAll(connection, new Filter(sql, params));
    }

    public static <T> T populateFrom(ResultSet rs, T t) {
        Reflect.getFields(t.getClass()).filter(f -> !Modifier.isTransient(f.getModifiers())).stream()
                .forEach(f -> handle(() -> f.set(t, mapFromRs(rs, f.getType(), f.getName()))));
        return t;
    }

    public AtkEntities<T> getAll(DataSource dataSource, Filter filter) {
        return runAndReturn(dataSource, c -> getAll(c, filter));
    }

    public AtkEntities<T> getAll(DataSource dataSource, String sql, Object... params) {
        return runAndReturn(dataSource, c -> getAll(c, sql, params));
    }

    public AtkEntities<T> getAll(DataSource dataSource) {
        return runAndReturn(dataSource, c -> getAll(c, new Filter()));
    }

    public AtkEntities<T> getAllBySet(Connection connection) {
        return getAll(connection, new Filter(AND, entity.getEnFields().getSet()));
    }

    public AtkEntities<T> getAllBySet(DataSource dataSource) {
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

    public T retrieveBySet(DataSource dataSource, CallNilRet<RuntimeException> call) {
        Optional<T> optional = getBySet(dataSource);
        Assert.isTrue(optional.isPresent(), call);
        return optional.get();
    }

    public T retrieveBySet(DataSource dataSource) {
        return retrieveBySet(dataSource, () ->
                new RuntimeException(String.format(
                        "Unable to retrieve entity %s by set fields %s", entity.getTableName()
                        , entity.getEnFields().getSet().toString())));
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

    public Query setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Query setOrderBy(OrderBy orderByType, AtkEnField... orderBys) {
        this.orderByType = orderByType;
        this.orderBy = new AtkEnFields(orderBys);
        return this;
    }

    public Query setOrderBy(AtkEnField... orderBys) {
        return setOrderBy(DESC, orderBys);
    }

}
