package com.reactivecassandra.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RetryPolicyWithStat implements RetryPolicy, ReportProvider {
    private final RetryPolicy delegate;

    private final ConcurrentMap<List<Object>, Stat> stats = Maps.newConcurrentMap();

    public RetryPolicyWithStat(RetryPolicy delegate) {
        this.delegate = Preconditions.checkNotNull(delegate);
    }

    protected RetryPolicy delegate() {
        return delegate;
    }

    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        getStat(
                "onReadTimeout",
                CassandraUtils.getQuery(statement),
                cl,
                requiredResponses,
                receivedResponses,
                dataRetrieved,
                nbRetry
        ).inc();

        return delegate().onReadTimeout(statement, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        getStat(
                "onWriteTimeout",
                CassandraUtils.getQuery(statement),
                cl,
                writeType,
                requiredAcks,
                receivedAcks,
                nbRetry
        ).inc();

        return delegate().onWriteTimeout(statement, cl, writeType, requiredAcks, receivedAcks, nbRetry);
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        getStat(
                "onUnavailable",
                CassandraUtils.getQuery(statement),
                cl,
                requiredReplica,
                aliveReplica,
                nbRetry
        ).inc();

        return delegate().onUnavailable(statement, cl, requiredReplica, aliveReplica, nbRetry);
    }

    private Stat getStat(Object... keys) {
        return stats.computeIfAbsent(Arrays.asList(keys), o -> new Stat());
    }

    @Override
    public List<Map<String, Object>> getReport(Map<String, String> args) {
        List<Map.Entry<List<Object>, Map<String, Object>>> entries = stats.entrySet().stream()
                .<Map.Entry<List<Object>, Map<String, Object>>>map(e -> {
                    return Maps.immutableEntry(e.getKey(), ImmutableMap.of("count", e.getValue().counter.get()));
                })
                .collect(Collectors.toList());
        entries.sort(Comparator.comparing(Map.Entry::getKey, (key1, key2) -> {
            for (int i = 0; i < Math.min(key1.size(), key2.size()); i++) {
                Object o1 = key1.get(i);
                Object o2 = key2.get(i);
                if (o1 instanceof Comparable && o2 instanceof Comparable && o1.getClass() == o2.getClass()) {
                    @SuppressWarnings("unchecked") int result = ((Comparable) o1).compareTo(o2);
                    if (result != 0) {
                        return result;
                    }
                }
            }
            return 0;
        }));
        return Arrays.asList(
                ImmutableMap.of("delegate", String.valueOf(delegate())),
                ImmutableMap.of("report", entries)
        );
    }

    @Override
    public String toString() {
        return "RetryPolicyWithStat{" +
                "delegate=" + delegate +
                '}';
    }

    static class Stat {
        private final AtomicInteger counter = new AtomicInteger();

        void inc() {
            counter.incrementAndGet();
        }
    }
}
