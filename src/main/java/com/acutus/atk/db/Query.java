package com.acutus.atk.db;

import com.acutus.atk.db.sql.Filter;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.Strings;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

    private Strings getLeftJoin(int cnt,AbstractAtkEntity entity) {
        return getEagerFields(entity).map(f -> handle(() -> {

            // find the entity relation field
            Optional<Field> enRefField = entity.getRefFields().get(f.getName() + "Ref");
            Assert.isTrue(enRefField.isPresent(), "Field not found " + f.getName() + "Ref");
            return getLeftJoin(cnt,entity, (AtkEnRelation) enRefField.get().get(entity));
        })).collect(Collectors.toCollection(Strings::new));
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

        private String prepareSql(Filter filter) {
        String sql = filter.isCustom() ?
                filter.getCustomSql()
                : String.format("select %s from %s where %s %s"
                , entity.getEnFields().excludeIgnore().getTableAndColName().toString(",")
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
    public void getAll(Connection connection, Filter filter,CallOne<T> iterate,int limit) {
        String sql = prepareSql(filter).replaceAll("\\p{Cntrl}", " ");
        // add all the left joins
        sql = sql.substring(sql.toLowerCase().indexOf(" from "));
        Strings lj = getLeftJoin(0,entity);
        Strings split = Strings.asList(sql.replace(","," , ").split("\\s+"));
        split.add(split.indexOfIgnoreCase(entity.getTableName())+1,lj.toString(" "));
        if (!lj.isEmpty()) {
            split.add(1, IntStream.range(0, lj.size()).mapToObj(i -> getTmpTablename(i) + ".*")
                    .reduce((t1, t2) -> t1 + ", " + t2).get());
            split.add(0,",");
        }
        sql = "select " + entity.getTableName()+".* "+ split.toString(" ");
        // transform the select *
        Map<String, AbstractAtkEntity> map = new HashMap<>();
        T  lastEntity = null;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            filter.prepare(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next() && (limit > 0 || limit < 0)) {
                    Two<AbstractAtkEntity,Boolean> value = loadCascade(-1,map, entity, rs);
                    lastEntity = (lastEntity == null) ? (T) value.getFirst() : lastEntity;
                    if (!value.getSecond()) {
                        limit--;
                        iterate.call(lastEntity);
                        lastEntity = (T) value.getFirst();
                    }
                }
            }
        }
        if (lastEntity != null) {
            iterate.call(lastEntity);
        }
    }

    public void getAll(DataSource dataSource, Filter filter,CallOne<T> iterate,int limit) {
        run(dataSource,c -> {
            getAll(c,filter,iterate,limit);
        });
    }

    public void getAll(DataSource dataSource,CallOne<T> iterate) {
        getAll(dataSource,new Filter(AND, entity.getEnFields().getSet()),iterate,-1);
    }

    private  AtkEntities<T> getAll(Connection connection, Filter filter,int limit) {
        AtkEntities<T> entities = new AtkEntities<>();
        getAll(connection, filter, t -> entities.add(t),limit);
        return entities;
    }

    public AtkEntities<T> getAll(Connection connection, String sql, Object... params) {
        return getAll(connection, new Filter(sql, params),-1);
    }

    @SneakyThrows
    public Optional<T> get(Connection connection, Filter filter) {
        AtkEntities<T> entities = getAll(connection,filter,1);
        return Optional.ofNullable(!entities.isEmpty() ? entities.get(0): null);
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

    public static <T> T populateFrom(ResultSet rs, T t) {
        Reflect.getFields(t.getClass()).filter(f -> !Modifier.isTransient(f.getModifiers())).stream()
                .forEach(f -> handle(() -> f.set(t, mapFromRs(rs, f.getType(), f.getName()))));
        return t;
    }

    public AtkEntities<T> getAll(DataSource dataSource, Filter filter) {
        return runAndReturn(dataSource, c -> getAll(c, filter,-1));
    }

    public AtkEntities<T> getAll(DataSource dataSource, String sql, Object... params) {
        return runAndReturn(dataSource, c -> getAll(c, sql, params));
    }

    public AtkEntities<T> getAll(Connection connection) {
        return getAll(connection, new Filter(AND, entity.getEnFields().getSet()),-1);
    }

    public AtkEntities<T> getAll(DataSource dataSource) {
        return runAndReturn(dataSource, c -> getAll(c, new Filter(AND, entity.getEnFields().getSet()),-1));
    }

    public Optional<T> get(Connection connection) {
        return get(connection, entity.getEnFields().getSet());
    }

    @SneakyThrows
    private Optional<T> get(Connection connection, AtkEnFields set) {
        Assert.isTrue(!set.isEmpty(), "No set fields for entity %s ", entity.getTableName());
        return get(connection, and(set.toArray(new AtkEnField[]{})));
    }

    public Optional<T> get(DataSource dataSource) {
        return runAndReturn(dataSource, c -> get(c, entity.getEnFields().getSet()));
    }

    public Optional<T> findById(DataSource dataSource) {
        return runAndReturn(dataSource, c -> findById(c));
    }

    public T retrieve(DataSource dataSource, CallNilRet<RuntimeException> call) {
        Optional<T> optional = get(dataSource);
        Assert.isTrue(optional.isPresent(), call);
        return optional.get();
    }

    public T retrieve(DataSource dataSource) {
        return retrieve(dataSource, () ->
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
        return get(connection, ids);
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

    public static void main(String[] args) {
        String sql = "select * from quote,\n\t quote_item where..".replaceAll("\\p{Cntrl}", "").replace(","," , ");
        System.out.println(sql);
        System.out.println(Arrays.stream(sql.split("\\s+"))
                .reduce((w1,w2) -> w1 + "->" + w2).get());
    }
}
