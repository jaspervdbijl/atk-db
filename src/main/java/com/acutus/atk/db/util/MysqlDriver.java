package com.acutus.atk.db.util;

import com.acutus.atk.util.Assert;
import com.acutus.atk.util.collection.One;

import java.sql.Connection;
import java.util.List;

import static com.acutus.atk.db.sql.SQLHelper.query;

public class MysqlDriver extends AbstractDriver {

    public <T> T getLastInsertValue(Connection connection, Class<T> clazz) {
        List<One<T>> id = query(connection, clazz, "select LAST_INSERT_ID()");
        Assert.isTrue(!id.isEmpty(), "No inserted id found");
        return id.get(0).getFirst();
    }
}
