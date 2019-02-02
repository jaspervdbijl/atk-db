package com.acutus.atk.db.fe.keys;

import com.acutus.atk.db.AtkEnField;
import lombok.SneakyThrows;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.function.Predicate;

public class FrKeys extends ArrayList<FrKey> {

    @SneakyThrows
    public static FrKeys load(ResultSet rs) {
        FrKeys keys = new FrKeys();
        while (rs.next()) {
            FrKey fk = new FrKey();
            keys.add(fk.populate(rs, fk));
        }
        return keys;
    }

    public boolean containsField(AtkEnField field) {
        return stream().filter(k -> k.equals(field)).findAny().isPresent();
    }

    /**
     * @param filter
     * @return a new instance with items matching filter removed
     */
    public FrKeys removeWhen(Predicate<FrKey> filter) {
        FrKeys clone = new FrKeys();
        clone.addAll(this);
        clone.removeIf(filter);
        return clone;
    }

}
