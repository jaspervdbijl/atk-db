package com.acutus.atk.db;

import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor
public class AtkEntities<T extends AbstractAtkEntity> extends ArrayList<T> {

    public AtkEntities(Collection<? extends T> c) {
        super(c);
    }

    public List<?> toDao() {
        return stream().map(e -> e.toBase()).collect(Collectors.toList());
    }
}
