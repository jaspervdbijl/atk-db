package com.acutus.atk.db;

import com.acutus.atk.entity.AtkFieldList;
import com.acutus.atk.util.Strings;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@NoArgsConstructor
public class AtkEnFieldList extends AtkFieldList<AtkEnField> {

    public AtkEnFieldList(Collection<AtkEnField> collection) {
        addAll(collection);
    }

    public AtkEnFieldList(AtkEnField ... fields) {
        this(Arrays.asList(fields));
    }

    @Override
    public AtkEnFieldList getChanged() {
        return stream().filter(f -> f.isChanged()).collect(Collectors.toCollection(AtkEnFieldList::new));
    }

    @Override
    public AtkEnFieldList getSet() {
        return stream().filter(f -> f.isSet()).collect(Collectors.toCollection(AtkEnFieldList::new));
    }

    public void reset() {
        stream().forEach(f -> f.reset());
    }

    public AtkEnFieldList getIds() {
        return this.stream().filter(f -> f.isId()).collect(Collectors.toCollection(AtkEnFieldList::new));
    }

    public List getValues() {
        return this.stream().map(f -> f.get()).collect(Collectors.toList());
    }

    public Strings getColNames() {
        return new Strings(stream().map(f -> f.getColName()).collect(Collectors.toList()));
    }

    public Optional<AtkEnField> getByColName(String name) {
        Optional<AtkEnField> field = stream().filter(f -> f.getColName().equalsIgnoreCase(name)).findAny();
        return field;
    }

    public AtkEnFieldList clone() {
        return new AtkEnFieldList(this);
    }

    @Override
    public String toString() {
        return new Strings(stream().map(f -> f.toString()).collect(Collectors.toList())).toString("\n\t");

    }

}
