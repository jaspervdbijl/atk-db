package com.acutus.atk.db;

import com.acutus.atk.db.driver.AbstractDriver;
import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.db.sql.SQLHelper;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.util.Strings;
import com.acutus.atk.util.call.CallOne;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.acutus.atk.db.sql.SQLHelper.*;
import static com.acutus.atk.util.AtkUtil.handle;

public class View<T extends View> {

    @SneakyThrows
    private AtkEntities getEntities() {
        return Reflect.getFields(getClass()).filterType(AbstractAtkEntity.class).stream()
                .map(f -> handle(() -> {
                    if (f.get(this) == null) {
                        f.set(this, f.getType().getConstructor().newInstance());
                    }
                    return (AbstractAtkEntity) f.get(this);
                })).collect(Collectors.toCollection(AtkEntities::new));
    }

    @SneakyThrows
    private void ignoreMissingFields(AtkEntities entities, ResultSetMetaData rsMeta) {
        Strings colNames = IntStream.range(1, rsMeta.getColumnCount() + 1)
                .mapToObj(i -> handle(() -> rsMeta.getTableName(i) + "." +
                        rsMeta.getColumnName(i))).collect(Collectors.toCollection(Strings::new));

        entities.stream().forEach(e -> ((AbstractAtkEntity) e).getEnFields().stream()
                .filter(f -> !colNames.containsIgnoreCase(f.getTableAndColName())
                        && !colNames.containsIgnoreCase("." + f.getColName()))
                .forEach(f -> f.setIgnore(true)));
    }

    private String getColNames() {
        return (String) getEntities().stream()
                .flatMap(e -> ((AbstractAtkEntity) e).getEnFields().stream())
                .filter(f -> !((AtkEnField) f).isIgnore())
                .map(f -> ((AtkEnField<?, ?>) f).getTableAndColName())
                .reduce((s1, s2) -> s1 + "," + s2).get();
    }

    @SneakyThrows
    public void iterate(Connection connection, String sql, CallOne<T> call, Object... params) {
        AtkEntities entities = getEntities();
        AbstractDriver driver = DriverFactory.getDriver(connection);

        sql = sql.replaceAll("\\p{Cntrl}", " ");

        String select = sql.substring(sql.toLowerCase().indexOf("select ") + "select ".length());
        select = select.substring(0, select.toLowerCase().indexOf(" from "));

        String cols = select.trim().equals("*") ? getColNames() : select;
        sql = "select " + cols + sql.substring(sql.toLowerCase().indexOf(" from "));

        // reformat is * is selected
        try (PreparedStatement ps = SQLHelper.prepare(connection, sql, params)) {
            try (ResultSet rs = ps.executeQuery()) {
                ignoreMissingFields(entities, rs.getMetaData());
                while (rs.next()) {
                    entities.stream().forEach(e -> ((AbstractAtkEntity) e).set(driver, rs));
                    call.call((T) this);
                }
            }
        }
    }

    @SneakyThrows
    public T clone() {
        T view = (T) getClass().getConstructor().newInstance();
        Reflect.getFields(getClass())
                .filter(f -> AbstractAtkEntity.class.isAssignableFrom(f.getType()))
                .forEach(f -> handle(() -> f.set(view, ((AbstractAtkEntity) f.get(this))
                        .clone())));
        return view;
    }

    public List<T> getAll(Connection connection, String sql, Object... params) {
        List<T> values = new ArrayList<>();
        iterate(connection, sql, (CallOne<T>) view -> values.add((T) view.clone()), params);
        return values;
    }

    public List<T> getAll(DataSource dataSource, String sql, Object... params) {
        return runAndReturn(dataSource, c -> getAll(c, sql, params));
    }

    @SneakyThrows
    private <T> boolean observeMatch(Class observable, T v1, T v2) {
        Field obField = Reflect.getFields(getClass())
                .filterType(AbstractAtkEntity.class).stream().filter(c -> c.getType().equals(observable)).findFirst().get();
        AbstractAtkEntity e1 = (AbstractAtkEntity) obField.get(v1);
        AbstractAtkEntity e2 = (AbstractAtkEntity) obField.get(v2);
        return e1.getEnFields().getIds().isEqual(e2.getEnFields().getIds());
    }

    @SneakyThrows
    public void observe(Connection connection, Class observable, String sql, CallOne<List<T>> call, Object... params) {
        List<T> values = new ArrayList<>();
        iterate(connection, sql, (value) -> {
            if (!values.isEmpty()) {
                if (!observeMatch(observable, values.get(values.size() - 1), value)) {
                    call.call(values);
                    values.clear();
                }
            }
            values.add((T) value.clone());
        }, params);
        if (!values.isEmpty()) {
            call.call(values);
        }
    }

}
