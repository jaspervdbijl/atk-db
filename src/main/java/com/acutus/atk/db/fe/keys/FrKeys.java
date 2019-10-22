package com.acutus.atk.db.fe.keys;

import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.util.Strings;
import lombok.SneakyThrows;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.acutus.atk.db.Query.populateFrom;

public class FrKeys extends ArrayList<FrKey> {

    @SneakyThrows
    public static FrKeys load(ResultSet rs) {
        FrKeys keys = new FrKeys();
        while (rs.next()) {
            FrKey fk = new FrKey();
            keys.add(populateFrom(rs, fk));
        }
        return keys;
    }

    public Strings getFkColNames() {
        return stream().map(f -> f.getFKCOLUMN_NAME()).collect(Collectors.toCollection(Strings::new));
    }

    public boolean containsField(AtkEnField field) {
        return stream().filter(k -> k.equals(field)).findAny().isPresent();
    }

    public int indexOf(AtkEnField field) {
        for (int i = 0;i < size();i++) {
            if (get(i).equals(field)) {
                return i;
            }
        }
        return -1;
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
