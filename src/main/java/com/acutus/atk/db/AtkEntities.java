package com.acutus.atk.db;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AtkEntities<T extends AbstractAtkEntity> extends ArrayList<T> {

    public List<?> toDao() {
        return stream().map(e -> e.toBase()).collect(Collectors.toList());
    }
}
