package com.acutus.atk.db;

import com.acutus.atk.db.annotations.Index;
import com.acutus.atk.util.Strings;

import java.util.ArrayList;
import java.util.Optional;

public class AtkEnIndexes extends ArrayList<AtkEnIndex> {

    public Optional<AtkEnIndex> getByName(String name) {
        return stream().filter(i -> i.getName().equalsIgnoreCase(name)).findAny();
    }

}
