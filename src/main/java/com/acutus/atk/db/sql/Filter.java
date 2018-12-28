package com.acutus.atk.db.sql;

import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFieldList;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicInteger;

@NoArgsConstructor
public class Filter {

    public enum Type {
        AND, OR, NOT
    }

    private AtkEnFieldList fields;
    private Filter s1, s2;
    private Type type;


    public Filter(Type type, AtkEnField[] fields) {
        this.type = type;
        this.fields = new AtkEnFieldList(fields);
    }

    public Filter(Type type, Filter s1, Filter s2) {
        this.type = type;
        this.s1 = s1;
        this.s2 = s2;
    }

    public static Filter and(AtkEnField... fields) {
        return new Filter(Type.AND, fields);
    }

    public static Filter and(Filter s1, Filter s2) {
        return new Filter(Type.AND, s1, s2);
    }

    public static Filter or(AtkEnField... fields) {
        return new Filter(Type.OR, fields);
    }

    public static Filter or(Filter s1, Filter s2) {
        return new Filter(Type.OR, s1, s2);
    }

    public static Filter not(AtkEnField... fields) {
        return new Filter(Type.NOT, fields);
    }

    public static Filter not(Filter s1, Filter s2) {
        return new Filter(Type.NOT, s1, s2);
    }

    public String getSql() {
        if (fields != null) {
            return fields.getColNames().append(" = ? ")
                    .toString(String.format(" %s ", type.name().toLowerCase()));
        } else {
            return String.format("((%s) %s (%s))", s1.getSql(), type.name().toLowerCase(), s2.getSql());
        }
    }

    @SneakyThrows
    private void set(PreparedStatement ps, AtomicInteger index) {
        if (fields != null) {
            for (AtkEnField f : fields) {
                ps.setObject(index.getAndIncrement(), f.get());
            }
        } else {
            s1.set(ps, index);
            s2.set(ps, index);
        }
    }

    public PreparedStatement prepare(PreparedStatement ps) {
        set(ps, new AtomicInteger(1));
        return ps;
    }


}