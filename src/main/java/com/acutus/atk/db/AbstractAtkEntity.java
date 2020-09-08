package com.acutus.atk.db;

import com.acutus.atk.db.driver.AbstractDriver;
import com.acutus.atk.db.processor.AtkEntity;
import com.acutus.atk.entity.AbstractAtk;
import com.acutus.atk.entity.AtkFieldList;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.util.StringUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import javax.persistence.Table;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.acutus.atk.db.Query.*;
import static com.acutus.atk.util.AtkUtil.handle;

public class AbstractAtkEntity<T extends AbstractAtkEntity, O> extends AbstractAtk<T, O> {

    @Getter
    @Setter
    private transient boolean isLoadedFromDB;

    private transient AtkEnIndexes indexes;

    @Setter
    @Getter
    private String tableName;

    protected void addIndex(AtkEnIndex index) {
        getIndexes().add(index);
    }

    public AtkEnIndexes getIndexes() {
        indexes = indexes == null ? new AtkEnIndexes() : indexes;
        return indexes;
    }

    public String getTableName() {
        if (tableName == null) {
            Table table = getClass().getAnnotation(Table.class);
            tableName = table != null && !StringUtils.isEmpty(table.name()) ? table.name() :
            getEntityType() == AtkEntity.Type.VIEW ? "" : getClass().getSimpleName();
        }
        return tableName;
    }

    public boolean isIdEqual(AbstractAtkEntity entity) {
        return getClass().equals(entity.getClass()) &&
                getEnFields().getIds().isEqual(entity.getEnFields().getIds());
    }

    public boolean hasIdValue() {
        return !getEnFields().getIds().getValues().contains(null);
    }

    public int version() {
        return 0;
    }

    public AtkEnFields getEnFields() {
        return new AtkEnFields(getFields());
    }

    @SneakyThrows
    public T clone() {
        T clone = (T) super.clone();
        // copy fetch types
        for (Field field : Reflect.getFields(getClass()).filterType(AtkEnRelation.class)) {
            ((AtkEnRelation) field.get(clone)).setFetchType(((AtkEnRelation) field.get(this)).getFetchType());
        }
        return clone;
    }

    @SneakyThrows
    public T set(AbstractDriver driver, ResultSet rs) {
        isLoadedFromDB = true;
        getEnFields().excludeIgnore().stream().forEach(f -> {
            f.setFromRs(driver, rs);
            f.setSet(false);
        });
        return (T) this;
    }

    public O toBase() {
        O base = super.toBase();
        // process eager fetches and map them to their dao's
        getOneToMany(this).forEach(f -> handle(() -> {
            if (f.get(this) != null) {
                List<AbstractAtkEntity> values = (List<AbstractAtkEntity>) f.get(this);
                Reflect.getFields(base.getClass()).get(f.getName()).get()
                        .set(base, values.stream().map(v -> v.toBase()).collect(Collectors.toList()));
            }
        }));
        getOneToOneOrManyToOne(this).forEach(f -> handle(() -> {
            if (f.get(this) != null && ((Optional) f.get(this)).isPresent()) {
                AbstractAtkEntity value = (AbstractAtkEntity) ((Optional) f.get(this)).get();
                Reflect.getFields(base.getClass()).get(f.getName()).get().set(base, value.toBase());
            }
        }));
        return base;
    }

    @SneakyThrows
    public O toBaseAndFetchEager(DataSource dataSource) {
        O base = super.toBase();
        // recursively retrieve all the eager loads
        getRefFields().filterType(AtkEnRelation.class).forEach(f -> {
            handle(() -> {
                        Field lField = Reflect.getFields(base.getClass())
                                .get(f.getName().substring(0, f.getName().length() - "Ref".length())).get();
                        List<AbstractAtkEntity> values = ((AtkEnRelation) f.get(this)).getAll(dataSource);
                        List converted = values.stream().map(v -> v.toBaseAndFetchEager(dataSource)).collect(Collectors.toList());
                        lField.set(base, converted);

                    }
            );
        });
        return base;
    }

    public T initFrom(O base) {
        return super.initFrom(base, new AtkFieldList());
    }

    public Persist<T> persist() {
        return new Persist(this);
    }

    public Query<T, O> query() {
        return new Query(this);
    }

    public AtkEntity.Type getEntityType() {
        throw new RuntimeException("Not implemented");
    }

}
