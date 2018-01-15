package com.reactivecassandra.util;

import com.google.common.base.Preconditions;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CollectUtils {
    private CollectUtils() {
    }

    /**
     * Collector to LinkedHashSet
     *
     * @param <T>
     * @return
     */
    public static <T> Collector<T, ?, LinkedHashSet<T>> toLinkedHashSet() {
        return Collectors.toCollection(LinkedHashSet::new);
    }

    /**
     * Collector to TreeSet
     *
     * @param <T>
     * @return
     */
    public static <T extends Comparable<T>> Collector<T, ?, TreeSet<T>> toTreeSet() {
        return Collectors.toCollection(TreeSet::new);
    }

    /**
     * Collector to TreeSet
     *
     * @param <T>
     * @return
     */
    public static <T> Collector<T, ?, TreeSet<T>> toTreeSet(Comparator<? super T> comparator) {
        Preconditions.checkNotNull(comparator, "comparator is null");
        return Collectors.toCollection(() -> new TreeSet<>(comparator));
    }

    /**
     * Collects the first (of Iterator) key value on merge
     *
     * @param keyMapper
     * @param valueMapper
     * @param <K>
     * @param <T>
     * @return
     */
    public static <T, K, V> Collector<T, ?, Map<K, V>> toMapFirst(Function<? super T, ? extends K> keyMapper,
                                                                  Function<? super T, ? extends V> valueMapper) {
        return Collectors.toMap(keyMapper, valueMapper, firstMerger(), HashMap::new);
    }

    /**
     * Collects the last (of Iterator) key value on merge
     *
     * @param keyMapper
     * @param valueMapper
     * @param <K>
     * @param <T>
     * @return
     */
    public static <T, K, V> Collector<T, ?, Map<K, V>> toMapLast(Function<? super T, ? extends K> keyMapper,
                                                                 Function<? super T, ? extends V> valueMapper) {
        return Collectors.toMap(keyMapper, valueMapper, lastMerger(), HashMap::new);
    }

    /**
     * Collector to LinkedHashMap
     *
     * @param keyMapper
     * @param valueMapper
     * @param <T>
     * @param <K>
     * @param <V>
     * @return
     * @throws IllegalStateException if mapped key is not unique
     */
    public static <T, K, V> Collector<T, ?, LinkedHashMap<K, V>> toLinkedHashMap(Function<? super T, ? extends K> keyMapper,
                                                                                 Function<? super T, ? extends V> valueMapper) {
        return Collectors.toMap(keyMapper, valueMapper, throwingMerger(), LinkedHashMap::new);
    }

    /**
     * Collector to LinkedHashMap of Map.Entry Stream
     *
     * @param <K>
     * @param <V>
     * @return
     * @throws IllegalStateException if mapped key is not unique
     */
    public static <K, V> Collector<Map.Entry<K, V>, ?, LinkedHashMap<K, V>> toLinkedHashMap() {
        return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, throwingMerger(), LinkedHashMap::new);
    }

    /**
     * Collector to TreeMap with natural ordering
     *
     * @param keyMapper
     * @param valueMapper
     * @param <T>
     * @param <K>
     * @param <V>
     * @return
     * @throws IllegalStateException if mapped key is not unique
     */
    public static <T, K extends Comparable<K>, V> Collector<T, ?, TreeMap<K, V>> toTreeMap(Function<? super T, ? extends K> keyMapper,
                                                                                           Function<? super T, ? extends V> valueMapper) {
        return Collectors.toMap(keyMapper, valueMapper, throwingMerger(), TreeMap::new);
    }

    /**
     * Collector to TreeMap with comparator
     *
     * @param keyMapper
     * @param valueMapper
     * @param <T>
     * @param <K>
     * @param <V>
     * @return
     * @throws IllegalStateException if mapped key is not unique
     */
    public static <T, K, V> Collector<T, ?, TreeMap<K, V>> toTreeMap(Function<? super T, ? extends K> keyMapper,
                                                                     Function<? super T, ? extends V> valueMapper,
                                                                     Comparator<? super K> comparator) {
        Preconditions.checkNotNull(comparator, "comparator is null");
        return Collectors.toMap(keyMapper, valueMapper, throwingMerger(), () -> new TreeMap<>(comparator));
    }

    private static <T> BinaryOperator<T> firstMerger() {
        return (u, v) -> u;
    }

    private static <T> BinaryOperator<T> lastMerger() {
        return (u, v) -> v;
    }

    private static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key, existing value=[%s], new value=[%s]", u, v));
        };
    }
}
