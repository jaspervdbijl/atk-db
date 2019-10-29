package com.acutus.atk.db;

import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.entity.AtkFieldList;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.Strings;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@NoArgsConstructor
public class AtkEnFields extends AtkFieldList<AtkEnField> {

    public AtkEnFields(Collection<AtkEnField> collection) {
        addAll(collection);
    }

    public AtkEnFields(AtkEnField... fields) {
        this(Arrays.asList(fields));
    }

    @Override
    public AtkEnFields getChanged() {
        return stream().filter(f -> f.isChanged()).collect(Collectors.toCollection(AtkEnFields::new));
    }

    public AtkEnFields excludeIgnore() {
        return stream().filter(f -> !f.isIgnore()).collect(Collectors.toCollection(AtkEnFields::new));
    }


    @Override
    public AtkEnFields getSet() {
        return stream().filter(f -> f.isSet()).collect(Collectors.toCollection(AtkEnFields::new));
    }

    public void reset() {
        stream().forEach(f -> f.reset());
    }

    public AtkEnFields getIds() {
        return this.stream().filter(f -> f.isId()).collect(Collectors.toCollection(AtkEnFields::new));
    }

    public AtkEnField getSingleId() {
        AtkEnFields ids = getIds();
        Assert.isTrue(ids.size() == 1, "Expected a single id");
        return ids.get(0);
    }

    public AtkEnFields getForeignKeys() {
        return this.stream().filter(f -> f.isForeignKey()).collect(Collectors.toCollection(AtkEnFields::new));
    }

    public AtkEnFields getForeignKeys(Class table) {
        return this.stream()
                .filter(f -> f.isForeignKey() && f.getField().getAnnotation(ForeignKey.class).table().equals(table))
                .collect(Collectors.toCollection(AtkEnFields::new));
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

    public Optional<AtkEnField> getByFieldName(String name) {
        Optional<AtkEnField> field = stream().filter(f -> f.getField().getName().equalsIgnoreCase(name)).findAny();
        return field;
    }

    public AtkEnFields clone() {
        return new AtkEnFields(this);
    }

    /**
     * @param filter
     * @return a new instance with items matching filter removed
     */
    public AtkEnFields removeWhen(Predicate<AtkEnField> filter) {
        AtkEnFields clone = clone();
        clone.removeIf(filter);
        return clone;
    }

    @Override
    public String toString() {
        return new Strings(stream().map(f -> f.toString()).collect(Collectors.toList())).toString("\n\t");

    }

}
