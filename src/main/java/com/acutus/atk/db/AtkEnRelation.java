package com.acutus.atk.db;

import com.acutus.atk.db.annotations.FieldFilter;
import com.acutus.atk.db.sql.Filter;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.Strings;
import com.acutus.atk.util.call.CallOne;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import javax.persistence.FetchType;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Optional;

import static com.acutus.atk.db.sql.Filter.Type.AND;
import static com.acutus.atk.db.sql.SQLHelper.run;
import static com.acutus.atk.db.util.AtkEnUtil.getQuery;

public class AtkEnRelation<T extends AbstractAtkEntity> {

    public enum RelType {
        OneToOne, OneToMany, ManyToOne
    }

    @Getter
    private Class<T> type;
    @Getter
    private RelType relType;
    private AbstractAtkEntity source;
    private String fieldFilter;
    private Field selectFilter[];

    @Setter @Getter
    private FetchType fetchType;

    public AtkEnRelation(Class<T> type, RelType relType, AbstractAtkEntity source) {
        this.type = type;
        this.relType = relType;
        this.source = source;
    }

    public AtkEnRelation(Class<T> type, RelType relType, String fieldFilter, AbstractAtkEntity source) {
        this(type,relType,source);
        this.fieldFilter = fieldFilter;
    }

    private T getOneToMany(T instance) {
        AtkEnFields fField = instance.getEnFields().getForeignKeys(source.getClass());
        fField.get(0).set(source.getEnFields().getIds().get(0).get());
        return instance;
    }

    private T getManyToOne(T instance) {
        AtkEnFields fField = source.getEnFields().getForeignKeys(instance.getClass());
        AtkEnField key = fieldFilter != null ? fField.getByFieldName(fieldFilter).get() : fField.get(0);
        if (key.get() != null) {
            instance.getEnFields().getSingleId().set(key.get());
            return instance;
        } else {
            return null;
        }
    }

    private T getOneToOne(T instance) {
        if (!instance.getEnFields().getForeignKeys(source.getClass()).isEmpty()) {
            return getOneToMany(instance);
        } else {
            return getManyToOne(instance);
        }
    }

    public void lazy() {
        this.fetchType = FetchType.LAZY;
    }

    public void eager() {
        this.fetchType = FetchType.EAGER;
    }

    public boolean isEager() {
        return this.fetchType == FetchType.EAGER;
    }


    @SneakyThrows
    private T getEntity() {
        T instance = type.getConstructor().newInstance();

        Assert.isTrue(source.getEnFields().getIds().size() == 1, "Expected exactly one ID field in " + source);
        Assert.isTrue(instance.getEnFields().getIds().size() == 1, "Expected exactly one ID field in " + type);

        switch (relType) {
            case OneToMany:
                return getOneToMany(instance);
            case ManyToOne:
                return getManyToOne(instance);
            case OneToOne:
                return getOneToOne(instance);
        }
        return instance;
    }

    public AtkEntities<T> getAll(DataSource dataSource) {
        return getQuery(getEntity(),selectFilter).getAll(dataSource);
    }

    public AtkEntities<T> getAll(Connection c) {
        return getQuery(getEntity(),selectFilter).getAll(c);
    }

    public Optional<T> get(DataSource dataSource) {
        T entity = getEntity();
        return entity != null ? getQuery(entity,selectFilter).get(dataSource) : Optional.empty();
    }

    public Optional<T> get(Connection connection) {
        T entity = getEntity();
        return entity != null ? getQuery(entity,selectFilter).get(connection) : Optional.empty();
    }

    public void iterate(Connection connection, CallOne<T> call) {
        T entity = getEntity();
        getQuery(entity,selectFilter).getAll(connection, new Filter(AND, entity.getEnFields().getSet()), call, -1);
    }

    public void iterate(DataSource dataSource, CallOne<T> call) {
        run(dataSource, c -> iterate(c, call));
    }

    public AtkEnRelation<T> setSelectFilter(Field ... fields) {
        this.selectFilter = fields;
        return this;
    }


}
