package org.linkeddatafragments.characteristicset;

import org.linkeddatafragments.util.StarString;

public interface ICharacteristicSet {
    boolean matches(StarString starPattern);
    double count(StarString starPattern);
}
