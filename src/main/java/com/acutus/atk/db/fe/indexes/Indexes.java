package com.acutus.atk.db.fe.indexes;

import lombok.SneakyThrows;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.acutus.atk.db.Query.populateFrom;

public class Indexes extends ArrayList<Index> {

    @SneakyThrows
    public static Indexes load(ResultSet rs) {
        Indexes indexes = new Indexes();
        while (rs.next()) {
            Index idx = populateFrom(rs, new Index());
            Optional<Index> index = indexes.getByName(idx.getINDEX_NAME());
            if (index.isPresent()) {
                index.get().getColumns().add(idx.getCOLUMN_NAME());
            } else {
                idx.getColumns().add(idx.getCOLUMN_NAME());
                indexes.add(idx);
            }

        }
        return indexes;
    }

    public Optional<Index> getByName(String name) {
        return stream().filter(i -> i.getINDEX_NAME().equalsIgnoreCase(name)).findAny();
    }


}
