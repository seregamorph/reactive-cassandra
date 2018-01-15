package com.reactivecassandra.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.reactivecassandra.async.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Cassandra result set joiner
 *
 * @param <K>
 * @param <V>
 */
public class Joiner<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(Joiner.class);

    public static class Join<K, V> {
        final ListenableFuture<? extends ResultSet> resultSetFuture;
        final Function<? super Row, ? extends K> keyMapper;
        final BiFunction<? super Row, ? super V, ? extends V> processor;

        private Join(ListenableFuture<? extends ResultSet> resultSetFuture,
                     Function<? super Row, ? extends K> keyMapper,
                     BiFunction<? super Row, ? super V, ? extends V> processor) {
            this.resultSetFuture = resultSetFuture;
            this.keyMapper = keyMapper;
            this.processor = processor;
        }

        public static <K, V> Join<K, V> of(ListenableFuture<? extends ResultSet> resultSet,
                                           Function<? super Row, ? extends K> keyMapper,
                                           BiFunction<? super Row, ? super V, ? extends V> processor) {
            Preconditions.checkArgument(resultSet != null);
            Preconditions.checkArgument(keyMapper != null);
            Preconditions.checkArgument(processor != null);

            return new Join<>(resultSet, keyMapper, processor);
        }

        public static <K, V> Join<K, V> ofEmpty() {
            return new Join<>(null, null, null);
        }
    }

    private final Async<Map<K, V>> future;
    @Nullable
    private final String query;
    private final Function<? super Row, ? extends K> keyMapper;

    protected Joiner(Async<Map<K, V>> future, @Nullable String query, Function<? super Row, ? extends K> keyMapper) {
        this.future = Preconditions.checkNotNull(future, "future is null");
        this.query = query;
        this.keyMapper = Preconditions.checkNotNull(keyMapper, "keyMapper is null");
    }

    public static <K, V> Joiner<K, V> fromSelect(ListenableFuture<? extends ResultSet> resultSetFuture,
                                                 Function<? super Row, ? extends K> keyMapper,
                                                 Function<? super Row, ? extends V> valueMapper,
                                                 Supplier<? extends Map<K, V>> mapSupplier) {
        return new Joiner<>(Async.of(resultSetFuture).map(rs -> {
            Map<K, V> map = mapSupplier.get();
            for (Row row : rs) {
                K key = keyMapper.apply(row);
                V value = valueMapper.apply(row);
                V prevValue = map.put(key, value);

                if (prevValue != null) {
                    Async<?> async = resultSetFuture instanceof Async ? (Async<?>) resultSetFuture : null;
                    Throwable stackTrace = async != null ? async.getStackTrace() : null;
                    logger.error("map already contains key, replaced [" + key +
                            "] prevValue [" + prevValue + "] newValue [" + value + "]" +
                            (async != null ? " context " + async.getAttributes() : ""), stackTrace);
                }
            }
            return map;
        }), CassandraUtils.getQuery(resultSetFuture), keyMapper);
    }

    public static <K, V> Joiner<K, V> fromSelect(ListenableFuture<? extends ResultSet> resultSetFuture,
                                                 Function<? super Row, ? extends K> keyMapper,
                                                 Function<? super Row, ? extends V> valueMapper) {
        return fromSelect(resultSetFuture, keyMapper, valueMapper, Maps::newLinkedHashMap);
    }

    public Joiner<K, V> leftJoin(Join<? extends K, V> join) {
        if (join.resultSetFuture == null) {
            return this;
        }

        return new Joiner<>(future.flatMap(map -> {
            return Async.of(join.resultSetFuture).map(rs -> {
                rs.forEach(row -> {
                    K key = join.keyMapper.apply(row);
                    V value = map.get(key);
                    if (value == null) {
                        return;
                    }
                    V newValue = join.processor.apply(row, value);
                    map.put(key, newValue);
                });
                return map;
            });
        }), query, keyMapper);
    }

    public Joiner<K, V> leftJoin(ListenableFuture<? extends ResultSet> resultSet,
                                 Function<? super Row, ? extends K> keyMapper,
                                 BiFunction<? super Row, ? super V, ? extends V> processor) {
        return leftJoin(Join.of(resultSet, keyMapper, processor));
    }

    public Joiner<K, V> leftJoin(ListenableFuture<? extends ResultSet> resultSet,
                                 BiFunction<? super Row, ? super V, ? extends V> processor) {
        return leftJoin(resultSet, this.keyMapper, processor);
    }

    public Async<Map<K, V>> asMap() {
        return future;
    }

    public Async<Collection<V>> asValues() {
        return asMap().map(Map::values);
    }

    public Async<V> asSingleOrNull() {
        return asValues().map(v -> CassandraUtils.singleOrNull(query, v));
    }
}
