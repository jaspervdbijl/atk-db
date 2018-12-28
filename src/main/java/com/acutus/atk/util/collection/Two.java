package com.acutus.atk.util.collection;

import com.acutus.atk.util.Assert;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class Two<A, B> extends Collectable {

    private A a;
    private B b;

    public A getFirst() {
        return a;
    }

    public B getSecond() {
        return b;
    }

    @Override
    public Collectable initFromList(List values) {
        Assert.isTrue(values.size() > 1, "Values less then 2");
        this.a = (A) values.get(0);
        this.b = (B) values.get(1);
        return this;
    }
}
