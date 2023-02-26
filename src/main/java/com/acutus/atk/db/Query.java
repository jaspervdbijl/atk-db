package com.acutus.atk.db;

import com.acutus.atk.db.driver.AbstractDriver;
import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.db.processor.AtkEntity;
import com.acutus.atk.db.sql.Filter;
import com.acutus.atk.util.IOUtil;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.reflection.ReflectFields;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.Strings;
import com.acutus.atk.util.call.CallNilRet;
import com.acutus.atk.util.call.CallOne;
import com.acutus.atk.util.collection.Tuple3;
import com.acutus.atk.util.collection.Tuple2;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.FetchType;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.acutus.atk.db.Query.OrderBy.DESC;
import static com.acutus.atk.db.sql.Filter.Type.AND;
import static com.acutus.atk.db.sql.Filter.and;
import static com.acutus.atk.db.sql.SQLHelper.*;
import static com.acutus.atk.util.AtkUtil.getGenericFieldType;
import static com.acutus.atk.util.AtkUtil.handle;

@Slf4j
public class Query<T extends AbstractAtkEntity, O> {

    private static Map<String, String> RESOURCE_MAP = new ConcurrentHashMap<>();

    public enum OrderBy {
        ASC, DESC
    }

    private T entity;
    private int limit = -1, offset = 0;
    private AtkEnFields orderBy;
    private OrderBy orderByType;

    private boolean disableLeftJoin = false;

    @SneakyThrows
    public static  String getSqlResource(String name) {
        if (!RESOURCE_MAP.containsKey(name)) {
            RESOURCE_MAP.put(name, new String(IOUtil.readAvailable(Thread.currentThread().getContextClassLoader().getResourceAsStream(name))));
        }
        return RESOURCE_MAP.get(name);
    }

    public Query(T entity) {
        this.entity = (T) entity.clone();
    }

    public Query<T,O> disableLeftJoin() {
        disableLeftJoin = true;
        return this;
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

    private Tuple2<String, Boolean> keyToString(AbstractAtkEntity entity) {
        Optional<AtkEnField> hasNull = entity.getEnFields().getIds().stream().filter(f -> f.get() == null).findAny();
        return new Tuple2(entity.getTableName() + "_" + entity.getEnFields().getIds()
                .stream().map((f1 -> "" + f1.get())).reduce((o1, o2) -> o1 + "" + o2).get(), !hasNull.isEmpty());
    }

    /**
     * @param cnt
     * @param map
     * @param entity
     * @param rs
     * @return the mapped entity, if the entity has already been loaded into the map, and if the entity's id is null
     */
    @SneakyThrows
    private Tuple3<AbstractAtkEntity, Boolean, Boolean> loadCascade(AbstractDriver driver, AtomicInteger cnt, Map<String, AbstractAtkEntity> map, AbstractAtkEntity entity, ResultSet rs) {
        if (cnt.get() > -1) {
            entity.setTableName(getTmpTablename(cnt.get()));
        }
        entity.set(driver, rs);
        Tuple2<String, Boolean> keyIsNul = keyToString(entity);
        String key = keyIsNul.getFirst();
        boolean existed = map.containsKey(key);
        if (!map.containsKey(key)) {
            // if count is -1 then it means its a new row for the main entity and you can clear the map
            if (cnt.get() == -1) map.clear();
            map.put(key, entity.clone());
        }
        AbstractAtkEntity local = map.get(key);
        if (!disableLeftJoin) {
            for (Field f : getEagerFields(local)) {
                AbstractAtkEntity child = (AbstractAtkEntity) getGenericFieldType(f).getConstructor().newInstance();
                cnt.getAndIncrement();
                Tuple3<AbstractAtkEntity, Boolean, Boolean> value = loadCascade(driver, cnt, map, child, rs);
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
        return new Tuple3<>(local, existed, keyIsNul.getSecond());
    }

    private String prepareSql(Filter filter) {
        String sql = filter.isCustom() ?
                filter.getCustomSql()
                : String.format("select %s from %s where %s %s"
                , entity.getEnFields().excludeIgnore().getTableAndColName().toString(",")
                , entity.getTableName(), filter.getSql(),
                orderBy != null
                        ? "order by " + orderBy.getTableAndColName().toString(",") + " " + orderByType.name()
                        : ""
        );
        return sql;
    }

    private String getProcessedSql(Filter filter) {
        String sql = prepareSql(filter).replaceAll("\\p{Cntrl}", " ");

        String select = sql.substring(sql.toLowerCase().indexOf("select ") + "select ".length());
        select = select.substring(0, select.toLowerCase().indexOf(" from "));

        // add all the left joins
        sql = sql.substring(sql.toLowerCase().indexOf("from "));

        String lj = !disableLeftJoin ? getLeftJoin(new AtomicInteger(0), entity) : "";

        Strings split = Strings.asList(sql.replace(",", " , ").split("\\s+"));

        if (!lj.isEmpty()) {
            int offset = split.indexOfIgnoreCase(entity.getTableName());
            String selectLJ = IntStream.range(0, lj.split("left join").length - 1)
                    .mapToObj(i -> getTmpTablename(i) + ".*").reduce((t1, t2) -> t1 + ", " + t2).get();

            split.add(0, "," + selectLJ);
            split.add(2 + offset, lj);
        }

        String star = !disableLeftJoin && "*".equals(select) ? entity.getEnFields().excludeIgnore().getTableAndColName().toString(",") : select;
        return "select " + star + " " + split.toString(" ");
    }

    @SneakyThrows
    public void getAll(Connection connection, Filter filter, CallOne<T> iterate, int limit) {
        AbstractDriver driver = DriverFactory.getDriver(connection);
        boolean shouldLeftJoin = !disableLeftJoin&& entity.getEntityType() == AtkEntity.Type.TABLE;
        String sql = !shouldLeftJoin && limit > -1 ? driver.limit(getProcessedSql(filter), limit, offset) : getProcessedSql(filter);

        // transform the select *
        Map<String, AbstractAtkEntity> map = new HashMap<>();
        Tuple3<AbstractAtkEntity, Boolean, Boolean> lastEntity = null;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            filter.prepare(ps);
            long start = System.currentTimeMillis();
            log.debug(sql);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (!shouldLeftJoin) {
                        iterate.call((T) entity.set(driver, rs).clone());
                    } else {
                        Tuple3<AbstractAtkEntity, Boolean, Boolean> value = loadCascade(driver, new AtomicInteger(-1), map, entity, rs);
                        if (lastEntity != null && !value.getFirst().isIdEqual(lastEntity.getFirst())) {
                            iterate.call((T) lastEntity.getFirst());
                            if (--limit == 0) return;
                        }
                        lastEntity = value;
                    }
                }
                long finish = System.currentTimeMillis();
                if((finish - start)/1000 > 1) {
                    log.debug("{}, {}", (finish - start)/1000, sql);
                }
            } catch (Exception ex) {
                log.error("sql with error: " + sql);
                throw ex;
            }
        }
        if (lastEntity != null) {
            iterate.call((T) lastEntity.getFirst());
        }
    }

    public void getAll(DataSource dataSource, Filter filter, CallOne<T> iterate, int limit) {
        run(dataSource, c -> getAll(c, filter, iterate, limit));
    }

    public List<O> getAllToBase(Connection c,Filter filter) {
        List<O> results = new ArrayList<>();
        getAll(c, filter, t -> results.add((O) t.toBase()), limit);
        return results;
    }

    public List<O> getAllToBase(Connection c) {
        return getAllToBase(c,new Filter(AND, entity.getEnFields().getSet()));
    }

    public List<O> getAllToBase(Connection c,String sql,Object ...params) {
        return getAllToBase(c,new Filter(sql, params));
    }

    public List<O> getAllToBase(DataSource dataSource,String sql,Object ...params) {
        return runAndReturn(dataSource, c -> getAllToBase(c,sql,params));
    }

    public List<O> getAllToBase(DataSource dataSource) {
        return runAndReturn(dataSource, c -> getAllToBase(c));
    }

    public void getAll(Connection c, CallOne<T> iterate) {
        getAll(c, new Filter(AND, entity.getEnFields().getSet()), iterate, limit);
    }

    public void getAll(Connection c, CallOne<T> iterate,int limit, String sql,Object ... params) {
        getAll(c, new Filter(sql,params), iterate, limit);
    }


    public void getAll(DataSource dataSource, CallOne<T> iterate) {
        getAll(dataSource, new Filter(AND, entity.getEnFields().getSet()), iterate, limit);
    }

    public AtkEntities<T> getAll(Connection connection, Filter filter, int limit) {
        AtkEntities<T> entities = new AtkEntities<>();
        getAll(connection, filter, t -> entities.add(t), limit);
        return entities;
    }

    /**
     * get dao class
     *
     * @param connection
     * @param filter
     * @param type
     * @param limit
     * @param <D>
     * @return
     */
    @SneakyThrows
    public <D> List<D> getAll(Connection connection, Filter filter, Class<D> type, int limit) {
        List<D> entities = new ArrayList<>();
        Optional<Method> m = Reflect.getMethods(entity.getClass()).getByName("to" + type.getSimpleName());
        Assert.isTrue(m.isPresent(), "No Method to" + type.getSimpleName() + " found in class " + entity.getClass());
        getAll(connection, filter, t -> entities.add((D) m.get().invoke(t)), limit);
        return entities;
    }


    public AtkEntities<T> getAll(DataSource dataSource, Filter filter, int limit) {
        return runAndReturn(dataSource, c -> getAll(c, filter, limit));
    }

    public <D> List<D> getAll(DataSource dataSource, Filter filter, Class<D> type, int limit) {
        return runAndReturn(dataSource, c -> getAll(c, filter, type, limit));
    }


    public AtkEntities<T> getAll(Connection connection, Filter filter) {
        return getAll(connection, filter, limit);
    }

    public AtkEntities<T> getAll(Connection connection, String sql, Object... params) {
        return getAll(connection, new Filter(sql, params), limit);
    }

    public <D> List<D> getAll(Connection connection, Class<D> type, int limit, String sql, Object... params) {
        return getAll(connection, new Filter(sql, params), type, limit);
    }


    public AtkEntities<T> getAllFromResource(Connection connection, String resource, Object... params) {
        return getAll(connection, new Filter(getSqlResource(resource), params), limit);
    }

    public void getAllFromResource(Connection connection, CallOne<T> iterate, int limit, String resource, Object... params) {
        getAll(connection, new Filter(getSqlResource(resource), params), iterate, limit);
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
        return runAndReturn(dataSource, c -> getAll(c, filter, limit));
    }

    public AtkEntities<T> getAll(DataSource dataSource, String sql, Object... params) {
        return runAndReturn(dataSource, c -> getAll(c, sql, params));
    }

    public AtkEntities<T> getAll(Connection connection) {
        return getAll(connection, new Filter(AND, entity.getEnFields().getSet()), limit);
    }

    public AtkEntities<T> getAll(DataSource dataSource) {
        return runAndReturn(dataSource, c -> getAll(c, new Filter(AND, entity.getEnFields().getSet()), limit));
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

    public Query<T, O> setOffset(Integer offset) {
        this.offset = offset;
        return this;
    }

    public Query<T, O> setOrderBy(OrderBy orderByType, Field... orderBys) {
        this.orderByType = orderByType;
        List<String> obFields = Arrays.asList(orderBys).stream().map(f -> f.getName().substring(1)).collect(Collectors.toList());
        this.orderBy = new AtkEnFields(entity.getEnFields().filter(f -> obFields.contains(f.getField().getName())));
        return this;
    }


    public Query<T, O> setOrderBy(Field... orderBys) {
        return setOrderBy(DESC, orderBys);
    }


}
