package com.acutus.atk.db;

import com.acutus.atk.db.driver.AbstractDriver;
import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.db.processor.AtkEntity;
import com.acutus.atk.db.sql.Filter;
import com.acutus.atk.io.IOUtil;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.reflection.ReflectFields;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.SimpleTimer;
import com.acutus.atk.util.Strings;
import com.acutus.atk.util.call.CallNilRet;
import com.acutus.atk.util.call.CallOne;
import com.acutus.atk.util.collection.Three;
import com.acutus.atk.util.collection.Two;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.FetchType;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.acutus.atk.db.Query.OrderBy.DESC;
import static com.acutus.atk.db.driver.DriverFactory.getDriver;
import static com.acutus.atk.db.sql.Filter.Type.AND;
import static com.acutus.atk.db.sql.Filter.and;
import static com.acutus.atk.db.sql.Filter.or;
import static com.acutus.atk.db.sql.SQLHelper.*;
import static com.acutus.atk.util.AtkUtil.getGenericFieldType;
import static com.acutus.atk.util.AtkUtil.handle;
import static com.acutus.atk.util.StringUtils.*;

@Slf4j
public class Query<T extends AbstractAtkEntity, O> {

    private static Map<String,String> RESOURCE_MAP = new HashMap<>();

    public enum OrderBy {
        ASC, DESC
    }

    private T entity;
    private Integer limit;
    private AtkEnFields orderBy;
    private OrderBy orderByType;

    private AtkEnFields selectFilter;

    @SneakyThrows
    public static synchronized String getSqlResource(String name) {
        if (!RESOURCE_MAP.containsKey(name)) {
            RESOURCE_MAP.put(name,new String(IOUtil.readAvailable(Thread.currentThread().getContextClassLoader().getResourceAsStream(name))));
        }
        return RESOURCE_MAP.get(name);
    }

    public Query(T entity) {
        this.entity = (T) entity.clone();
    }

    private String getTmpTablename(int dept) {
        return "" + (char) (65 + dept);
    }

    @SneakyThrows
    private String getLeftJoinOneTo(AtomicInteger cnt, String tableName, AbstractAtkEntity entity, AtkEnRelation re, AbstractAtkEntity e) {
        AtkEnFields fk = e.getEnFields().getForeignKeys(entity.getClass());
        String realTableName = e.getTableName();
        e.setTableName(getTmpTablename(cnt.getAndIncrement()));
        Assert.isTrue(fk.size() == 1, "FK [" + fk.getColNames() + "] not located for " + entity.getTableName());
        return String.format("left join %s %s on %s.%s = %s", realTableName, e.getTableName(),
                tableName, entity.getEnFields().getSingleId().getColName(), fk.get(0).getTableAndColName()) + " " +
                getLeftJoin(cnt, e);
    }

    @SneakyThrows
    private String getLeftJoinManyToOne(AtomicInteger cnt, String tableName, AbstractAtkEntity entity, AtkEnRelation re, AbstractAtkEntity e) {
        AtkEnFields fk = entity.getEnFields().getForeignKeys(e.getClass());
        String realTableName = e.getTableName();
        e.setTableName(getTmpTablename(cnt.getAndIncrement()));
        Assert.isTrue(fk.size() == 1, "FK [" + fk.getColNames() + "] not located for " + entity.getTableName());
        return String.format("left join %s %s on %s =%s.%s", realTableName, e.getTableName(),
                fk.get(0).getTableAndColName(), e.getTableName(), entity.getEnFields().getSingleId().getColName()) + " " +
                getLeftJoin(cnt, e);
    }

    @SneakyThrows
    private String getLeftJoin(AtomicInteger cnt, AbstractAtkEntity entity, AtkEnRelation re) {
        String tableName = entity.getTableName();
        if (cnt.get() > 0) {
            tableName = getTmpTablename(cnt.get() - 1);
        }
        AbstractAtkEntity e = (AbstractAtkEntity) re.getType().getConstructor().newInstance();
        if (re.getRelType() == AtkEnRelation.RelType.OneToMany || re.getRelType() == AtkEnRelation.RelType.OneToOne) {
            return getLeftJoinOneTo(cnt, tableName, entity, re, e);
        } else {
            return getLeftJoinManyToOne(cnt, tableName, entity, re, e);
        }
    }

    public static ReflectFields getOneToMany(AbstractAtkEntity entity) {
        return entity.getRefFields().filter(f -> (f.getAnnotation(OneToMany.class) != null));
    }

    public static ReflectFields getOneToOneOrManyToOne(AbstractAtkEntity entity) {
        return entity.getRefFields().filter(f -> (f.getAnnotation(ManyToOne.class) != null || f.getAnnotation(OneToOne.class) != null));
    }

    @SneakyThrows
    public static ReflectFields getEagerFields(AbstractAtkEntity entity) {
        ReflectFields rFields = entity.getRefFields().filterType(AtkEnRelation.class);
        return entity.getRefFields().filter(f ->
                handle(() -> rFields.getNames().contains(f.getName() + "Ref") &&
                        ((AtkEnRelation) rFields.getByName(f.getName() + "Ref").get().get(entity)).isEager() ||
                        f.getAnnotation(OneToMany.class) != null && f.getAnnotation(OneToMany.class).fetch().equals(FetchType.EAGER) ||
                        f.getAnnotation(ManyToOne.class) != null && f.getAnnotation(ManyToOne.class).fetch().equals(FetchType.EAGER) ||
                        f.getAnnotation(OneToOne.class) != null && f.getAnnotation(OneToOne.class).fetch().equals(FetchType.EAGER)
                ));
    }

    @SneakyThrows
    private String getLeftJoin(AtomicInteger cnt, AbstractAtkEntity entity) {
        Strings leftJoin = new Strings();
        for (Field f : getEagerFields(entity)) {
            Optional<Field> enRefField = entity.getRefFields().get(f.getName() + "Ref");
            Assert.isTrue(enRefField.isPresent(), "Field not found " + f.getName() + "Ref");
            leftJoin.add(getLeftJoin(cnt, entity, (AtkEnRelation) enRefField.get().get(entity)));
        }
        return leftJoin.toString(" ");
    }

    private Two<String,Boolean> keyToString(AbstractAtkEntity entity) {
        Optional<AtkEnField> hasNull = entity.getEnFields().getIds().stream().filter(f -> f.get() == null).findAny();
        return new Two(entity.getTableName() + "_" + entity.getEnFields().getIds()
                .stream().map((f1 -> ""+f1.get())).reduce((o1, o2) -> o1 + "" + o2).get(),!hasNull.isEmpty());
    }

    /**
     *
     * @param cnt
     * @param map
     * @param entity
     * @param rs
     * @return the mapped entity, if the entity has alrady been loaded into the map, and if the entity's id is null
     */
    @SneakyThrows
    private Three<AbstractAtkEntity, Boolean, Boolean> loadCascade(AbstractDriver driver,AtomicInteger cnt, Map<String, AbstractAtkEntity> map, AbstractAtkEntity entity, ResultSet rs) {
        if (cnt.get() > -1) {
            entity.setTableName(getTmpTablename(cnt.get()));
        }
        entity.set(driver, rs);
        Two<String,Boolean> keyIsNul = keyToString(entity);
        String key = keyIsNul.getFirst();
        boolean existed = map.containsKey(key);
        if (!map.containsKey(key)) {
            map.put(key, entity.clone());
        }
        AbstractAtkEntity local = map.get(key);
        if (selectFilter == null) {
            for (Field f : getEagerFields(local)) {
                AbstractAtkEntity child = (AbstractAtkEntity) getGenericFieldType(f).getConstructor().newInstance();
                cnt.getAndIncrement();
                Three<AbstractAtkEntity, Boolean, Boolean> value = loadCascade(driver, cnt, map, child, rs);
                if (value.getThird()) {
                    // id was null
                    if (Optional.class.isAssignableFrom(f.getType())) {
                        f.set(local, Optional.empty());
                    } else {
                        f.set(local, new AtkEntities<>());
                    }
                } else {
                    // entity has not been loaded into the map
                    if (Optional.class.isAssignableFrom(f.getType())) {
                        f.set(local, f.get(local) == null ? Optional.of(value.getFirst()) : f.get(local));
                    } else {
                        f.set(local, f.get(local) == null ? new AtkEntities<>() : f.get(local));
                        ((List) f.get(local)).add(value.getFirst());
                    }
                }
            }
        }
        return new Three<>(local, existed, keyIsNul.getSecond());
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

    private String getProcessedSql(Filter filter) {
        String sql = prepareSql(filter).replaceAll("\\p{Cntrl}", " ");

        String select = sql.substring(sql.toLowerCase().indexOf("select ") + "select ".length());
        select = select.substring(0,select.toLowerCase().indexOf(" from "));

        // add all the left joins
        sql = sql.substring(sql.toLowerCase().indexOf("from "));

        String lj = selectFilter == null ? getLeftJoin(new AtomicInteger(0), entity) : "";

        Strings split = Strings.asList(sql.replace(",", " , ").split("\\s+"));

        if (!lj.isEmpty()) {
            int offset = split.indexOfIgnoreCase(entity.getTableName());
            String selectLJ = IntStream.range(0, lj.split("left join").length - 1)
                    .mapToObj(i -> getTmpTablename(i) + ".*").reduce((t1, t2) -> t1 + ", " + t2).get();

            split.add(0, "," + selectLJ);
            split.add(2 + offset, lj);
        }

        String star = selectFilter != null ? selectFilter.getColNames().toString(",") : select;
        return "select " + star + " " + split.toString(" ");
    }

    @SneakyThrows
    public void getAll(Connection connection, Filter filter, CallOne<T> iterate, int limit) {
        AbstractDriver driver = DriverFactory.getDriver(connection);
        boolean shouldLeftJoin = selectFilter == null && entity.getEntityType() == AtkEntity.Type.TABLE;
        String sql = getProcessedSql(filter);

        // transform the select *
        Map<String, AbstractAtkEntity> map = new HashMap<>();
        Three<AbstractAtkEntity, Boolean, Boolean> lastEntity = null;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            filter.prepare(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next() && (limit > 0 || limit < 0)) {
                    if (!shouldLeftJoin) {
                        iterate.call((T) entity.set(driver, rs).clone());
                    } else {
                        Three<AbstractAtkEntity, Boolean, Boolean> value = loadCascade(driver, new AtomicInteger(-1), map, entity, rs);
                        if (lastEntity != null &&
                                !value.getFirst().isIdEqual(lastEntity.getFirst())) {
                            limit--;
                            iterate.call((T) lastEntity.getFirst());
                        }
                        lastEntity = value;
                    }
                }
            }
        }
        if (lastEntity != null) {
            iterate.call((T) lastEntity.getFirst());
        }
    }

    public void getAll(DataSource dataSource, Filter filter, CallOne<T> iterate, int limit) {
        run(dataSource, c -> {
            getAll(c, filter, iterate, limit);
        });
    }

    public void getAll(DataSource dataSource, CallOne<T> iterate) {
        getAll(dataSource, new Filter(AND, entity.getEnFields().getSet()), iterate, -1);
    }

    public AtkEntities<T> getAll(Connection connection, Filter filter, int limit) {
        AtkEntities<T> entities = new AtkEntities<>();
        getAll(connection, filter, t -> entities.add(t), limit);
        return entities;
    }

    public AtkEntities<T> getAll(DataSource dataSource, Filter filter, int limit) {
        return runAndReturn(dataSource,c -> getAll(c,filter,limit));
    }

    public AtkEntities<T> getAll(Connection connection, Filter filter) {
        return getAll(connection, filter, -1);
    }

    public AtkEntities<T> getAll(Connection connection, String sql, Object... params) {
        return getAll(connection, new Filter(sql, params), -1);
    }

    public AtkEntities<T> getAllFromResource(Connection connection, String resource, Object... params) {
        return getAll(connection, new Filter(getSqlResource(resource), params), -1);
    }

    public void getAllFromResource(Connection connection, CallOne<T> iterate, int limit,String resource, Object... params) {
        getAll(connection, new Filter(getSqlResource(resource),params),iterate, limit);
    }


    @SneakyThrows
    public Optional<T> get(Connection connection, Filter filter) {
        AtkEntities<T> entities = getAll(connection, filter, 1);
        return Optional.ofNullable(!entities.isEmpty() ? entities.get(0) : null);
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
        return runAndReturn(dataSource, c -> getAll(c, filter, -1));
    }

    public AtkEntities<T> getAll(DataSource dataSource, String sql, Object... params) {
        return runAndReturn(dataSource, c -> getAll(c, sql, params));
    }

    public AtkEntities<T> getAll(Connection connection) {
        return getAll(connection, new Filter(AND, entity.getEnFields().getSet()), -1);
    }

    public AtkEntities<T> getAll(DataSource dataSource) {
        return runAndReturn(dataSource, c -> getAll(c, new Filter(AND, entity.getEnFields().getSet()), -1));
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

    public T retrieve(Connection c, CallNilRet<RuntimeException> call) {
        Optional<T> optional = get(c);
        Assert.isTrue(optional.isPresent(), call);
        return optional.get();
    }

    public T retrieve(DataSource dataSource, CallNilRet<RuntimeException> call) {
        return runAndReturn(dataSource, c -> retrieve(c, call));
    }

    public T retrieve(Connection c) {
        return retrieve(c, () ->
                new RuntimeException(String.format(
                        "Unable to retrieve entity %s by set fields %s", entity.getTableName()
                        , entity.getEnFields().getSet().toString())));
    }

    public T retrieve(DataSource dataSource) {
        return runAndReturn(dataSource, c -> retrieve(c));
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

    public Query<T, O> setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Query<T, O> setOrderBy(OrderBy orderByType, Field... orderBys) {
        this.orderByType = orderByType;
        List<String> obFields = Arrays.asList(orderBys).stream().map(f -> f.getName().substring(1)).collect(Collectors.toList());
        this.orderBy = new AtkEnFields(entity.getEnFields().filter(f -> obFields.contains(f.getField().getName())));
        return this;
    }

    public Query<T, O> setSelectFilter(Field... filter) {
        List<String> names = Arrays.stream(filter).map(f -> f.getName().substring(1)).collect(Collectors.toList());
        selectFilter = entity.getEnFields()
                .stream().filter(f -> f.isId() || names.contains(f.getField().getName()))
                .collect(Collectors.toCollection(AtkEnFields::new));
        // ignore all the rest
        entity.getEnFields().filter(f -> !selectFilter.contains(f)).forEach(f -> f.setIgnore(true));
        return this;
    }

    public Query<T, O> setOrderBy(Field... orderBys) {
        return setOrderBy(DESC, orderBys);
    }


}
