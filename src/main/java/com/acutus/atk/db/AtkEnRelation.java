package com.acutus.atk.db;

import com.acutus.atk.db.sql.Filter;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.call.CallOne;
import lombok.Getter;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Optional;

import static com.acutus.atk.db.sql.Filter.Type.AND;
import static com.acutus.atk.db.sql.SQLHelper.run;
import static com.acutus.atk.db.util.AtkEnUtil.getQuery;

public class AtkEnRelation<T extends AbstractAtkEntity> {

    public enum RelType {
        OneToOne,OneToMany,ManyToOne
    }

    @Getter
    private Class<T> type;
    private RelType relType;
    private AbstractAtkEntity source;

    public AtkEnRelation(Class<T> type, RelType relType, AbstractAtkEntity source) {
        this.type = type;
        this.relType = relType;
        this.source = source;
    }

    private T getOneToMany(T instance) {
        AtkEnFields fField = instance.getEnFields().getForeignKeys(source.getClass());
        fField.get(0).set(source.getEnFields().getIds().get(0).get());
        return instance;
    }

    private T getManyToOne(T instance) {
        AtkEnFields fField = source.getEnFields().getForeignKeys(instance.getClass());
        instance.getEnFields().getSingleId().set(fField.get(0).get());
        return instance;
    }

    private T getOneToOne(T instance) {
        if (!instance.getEnFields().getForeignKeys(source.getClass()).isEmpty()) {
            return getOneToMany(instance);
        } else {
            return getManyToOne(instance);
        }
    }

    @SneakyThrows
    private T getEntity() {
        T instance = type.getConstructor().newInstance();

        Assert.isTrue(source.getEnFields().getIds().size() == 1, "Expected exactly one ID field in " + source);
        Assert.isTrue(instance.getEnFields().getIds().size() == 1, "Expected exactly one ID field in " + type);

        switch (relType) {
            case OneToMany : getOneToMany(instance);break;
            case ManyToOne: getManyToOne(instance);break;
            case OneToOne: getOneToOne(instance);break;
        }
        return instance;
    }

    public AtkEntities<T> getAll(DataSource dataSource) {
        return getQuery(getEntity()).getAll(dataSource);
    }

    public AtkEntities<T> getAll(Connection c) {
        return getQuery(getEntity()).getAll(c);
    }

    public Optional<T> get(DataSource dataSource) {
        return getQuery(getEntity()).get(dataSource);
    }

    public Optional<T> get(Connection connection) {
        return getQuery(getEntity()).get(connection);
    }

    public void iterate(Connection connection, CallOne<T> call) {
        T entity = getEntity();
        getQuery(entity).getAll(connection, new Filter(AND, entity.getEnFields().getSet()), call,-1);
    }

    public void iterate(DataSource dataSource, CallOne<T> call) {
        run(dataSource, c -> iterate(c, call));
    }


}
