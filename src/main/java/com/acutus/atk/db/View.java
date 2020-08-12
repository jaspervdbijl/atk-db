package com.acutus.atk.db;

import com.acutus.atk.db.driver.AbstractDriver;
import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.db.sql.SQLHelper;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.Strings;
import com.acutus.atk.util.call.CallNil;
import com.acutus.atk.util.call.CallNilRet;
import com.acutus.atk.util.call.CallOne;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.acutus.atk.db.Query.populateFrom;
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
    public void iterate(Connection connection, String sql, CallOne call, Object... params) {
        AtkEntities entities = getEntities();
        AbstractDriver driver = DriverFactory.getDriver(connection);
        try (PreparedStatement ps = SQLHelper.prepare(connection, sql, params)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    entities.stream().forEach(e -> ((AbstractAtkEntity)e).set(driver, rs));
                    call.call(this);
                }
            }
        }
    }


}
