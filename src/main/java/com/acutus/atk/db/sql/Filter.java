package com.acutus.atk.db.sql;

import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFieldList;
import com.acutus.atk.db.processor.AtkEntity;
import com.sun.javafx.binding.StringFormatter;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Stack;

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
        return String.format("(%s)",
                fields != null ? fields.getColNames().toString(String.format(" %s ", type.name().toLowerCase()))
                        : String.format("(%s %s %s)", s1.getSql(), type.name().toLowerCase(),s2.getSql()));

    }


}
