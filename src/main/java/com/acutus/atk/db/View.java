package com.acutus.atk.db;

import com.acutus.atk.db.sql.SQLHelper;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.call.CallNil;
import com.acutus.atk.util.collection.One;
import com.acutus.atk.util.collection.Three;
import com.acutus.atk.util.collection.Two;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.acutus.atk.db.Query.populateFrom;
import static com.acutus.atk.db.sql.SQLHelper.run;
import static com.acutus.atk.util.AtkUtil.handle;

public class View {

    @SneakyThrows
    private List<? extends AbstractAtkEntity> getEntities() {
        return Reflect.getFields(getClass()).filterType(AbstractAtkEntity.class).stream()
                        .map(f -> handle(() -> {
                            if (f.get(this) == null) {
                                f.set(this,f.getType().getConstructor().newInstance());
                            }
                            return (AbstractAtkEntity) f.get(this);
                        })).collect(Collectors.toList());
    }

    @SneakyThrows
    public void iterate(Connection connection, String sql, CallNil call,Object ... params) {
        List<? extends AbstractAtkEntity> entities = getEntities();
        try (PreparedStatement ps = SQLHelper.prepare(connection,sql,params)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    entities.stream().forEach(e -> e.set(rs));
                    call.call();
                }
            }
        }
    }

    public List<List<? extends AbstractAtkEntity>> getAll(DataSource dataSource, String sql, Object ... params) {
        List<? extends AbstractAtkEntity> entities = getEntities();
        List<List<? extends AbstractAtkEntity>> list = new ArrayList<>();
        run(dataSource,c -> {
            iterate(c,sql,() -> {
                list.add(entities.stream().map(e -> (AbstractAtkEntity) e.clone()).collect(Collectors.toList()));
            },params);
        });
        return list;
    }

    public <T extends AbstractAtkEntity> List<One<T>> getOne(DataSource dataSource, String sql, Object ... params) {
        return getAll(dataSource,sql,params).stream().map(l -> new One<T>((T)l.get(0)))
                .collect(Collectors.toList());
    }

    public <T extends AbstractAtkEntity,U extends AbstractAtkEntity> List<Two<T,U>> getTwo(DataSource dataSource, String sql, Object ... params) {
        return getAll(dataSource,sql,params).stream().map(l -> new Two<T,U>((T)l.get(0),(U)l.get(1)))
                .collect(Collectors.toList());
    }

    public <T extends AbstractAtkEntity,U extends AbstractAtkEntity, V extends AbstractAtkEntity> List<Three<T,U,V>> getThree(DataSource dataSource, String sql, Object ... params) {
        return getAll(dataSource,sql,params).stream().map(l -> new Three<T,U,V>((T)l.get(0),(U)l.get(1),(V) l.get(2)))
                .collect(Collectors.toList());
    }

}
