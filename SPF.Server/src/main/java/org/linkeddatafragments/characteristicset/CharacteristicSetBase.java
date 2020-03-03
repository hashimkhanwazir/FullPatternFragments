package org.linkeddatafragments.characteristicset;

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

    public abstract void addDistinct(Map<String, Integer> element);
}
