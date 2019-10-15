package com.acutus.atk.db;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.acutus.atk.util.AtkUtil.handle;

public class AbstractAtkEntity<T extends AbstractAtkEntity, O> extends AbstractAtk<T, O> {

    @Getter
    @Setter
    private transient boolean isLoadedFromDB;

    @Getter
    private transient List<AtkEnIndex> indexes = new ArrayList<>();

    // entities created as child classes will have this reference set to the parent
    @Getter
    private transient AbstractAtkEntity parentEntity;

    protected void addIndex(AtkEnIndex index) {
        indexes.add(index);
    }

    public String getTableName() {
        Table table = getClass().getAnnotation(Table.class);
        return table != null && !StringUtils.isEmpty(table.name()) ? table.name() : getClass().getSimpleName();
    }

    public int version() {
        return 0;
    }

    public AtkEnFields getEnFields() {
        return new AtkEnFields(getFields());
    }

    public T set(ResultSet rs) {
        isLoadedFromDB = true;
        getEnFields().stream().forEach(f -> {
            f.setFromRs(rs);
            f.setSet(false);
        });
        return (T) this;
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

    @Override
    public T restoreSet() {
        getFields().restoreSet();
        return (T) this;
    }

    public Persist<T> persist() {
        return new Persist(this);
    }

    public Query<T> query() {
        return new Query(this);
    }

}
