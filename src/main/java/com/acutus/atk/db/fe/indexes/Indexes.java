package com.acutus.atk.db.fe.indexes;

import lombok.SneakyThrows;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.acutus.atk.db.Query.populateFrom;

public class Indexes extends ArrayList<Index> {

    @SneakyThrows
    public static Indexes load(ResultSet rs) {
        Indexes indexes = new Indexes();
        while (rs.next()) {
            Index index = new Index();
            indexes.add(populateFrom(rs, index));
        }
        return indexes;
    }

    public List<String> getPrimaryKeyNames() {
        return stream().filter(i -> i.getPK_NAME() != null).map(i -> i.getCOLUMN_NAME()).collect(Collectors.toList());
    }

}
