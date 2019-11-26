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

    @Getter
    private Class<T> type;
    private AbstractAtkEntity source;

    public AtkEnRelation(Class<T> type, AbstractAtkEntity source) {
        this.type = type;
        this.source = source;
    }

    @SneakyThrows
    private T getEntity(boolean oneToMany) {
        T instance = type.getConstructor().newInstance();

        Assert.isTrue(source.getEnFields().getIds().size() == 1, "Expected exactly one ID field in " + source);
        Assert.isTrue(instance.getEnFields().getIds().size() == 1, "Expected exactly one ID field in " + type);

        AtkEnFields fField = oneToMany
                ? instance.getEnFields().getForeignKeys(source.getClass())
                : source.getEnFields().getForeignKeys(instance.getClass());
        Assert.isTrue(instance.getEnFields().getIds().size() == 1, "No matching foreign key found in %s for %s"
                , source.getClass(), type);
        fField.get(0).set(source.getEnFields().getIds().get(0).get());
        return instance;
    }

    public AtkEntities<T> getAll(DataSource dataSource) {
        return getQuery(getEntity(true)).getAllBySet(dataSource);
    }

    public AtkEntities<T> getAll(Connection c) {
        return getQuery(getEntity(true)).getAllBySet(c);
    }

    public Optional<T> get(DataSource dataSource) {
        return getQuery(getEntity(false)).getBySet(dataSource);
    }

    public void iterate(Connection connection, CallOne<T> call) {
        T entity = getEntity(true);
        getQuery(entity).iterate(connection, new Filter(AND, entity.getEnFields().getSet()), call);
    }

    public void iterate(DataSource dataSource, CallOne<T> call) {
        run(dataSource, c -> iterate(c, call));
    }


}
