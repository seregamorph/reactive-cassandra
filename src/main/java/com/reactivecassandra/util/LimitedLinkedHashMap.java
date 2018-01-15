package com.reactivecassandra.util;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LinkedHashMap with limited size, removes the eldest entry on overflow
 */
@NotThreadSafe
public class LimitedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
    private final int maxSize;

    public LimitedLinkedHashMap(int maxSize) {
        Preconditions.checkArgument(maxSize > 0);
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxSize;
    }

    public final int getMaxSize() {
        return maxSize;
    }

    public static <K, V> LimitedLinkedHashMap<K, V> create(final int size) {
        return new LimitedLinkedHashMap<>(size);
    }
}
