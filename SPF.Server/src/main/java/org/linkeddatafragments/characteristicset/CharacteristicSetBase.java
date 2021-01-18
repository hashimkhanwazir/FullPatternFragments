package org.linkeddatafragments.characteristicset;

import org.linkeddatafragments.util.Tuple;

import java.util.Map;

public abstract class CharacteristicSetBase implements ICharacteristicSet {
    protected int distinct;

    public CharacteristicSetBase(int distinct) {
        this.distinct = distinct;
    }

    public CharacteristicSetBase() {
        this(0);
    }

    public abstract int countPredicate(String predicate);

    public abstract void addDistinct(Map<String, Tuple<Integer, Integer>> element);

    public abstract void setObjectCount(String predicate, int count);

    public abstract boolean containsPredicate(String predicate);

    public double getDistinct() {
        return (double)distinct;
    }
}
