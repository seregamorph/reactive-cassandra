package com.reactivecassandra.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.reactivecassandra.async.Async;
import com.reactivecassandra.async.CompleteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * DataStax Java driver Session extension (via delegate pattern).
 * It aggregates query statistics and contains PreparedStatement cache.
 * <p/>
 * Note: this class is designed to be used with prepared statements.
 */
public class CassandraSession extends AbstractSession implements ReportProvider {
    private static final Logger logger = LoggerFactory.getLogger(CassandraSession.class);

    private final CqlStat cqlStat = new CqlStat();

    private final Session delegate;
    private final CassandraCluster cluster;
    private final Cache<List<Object>, Async<PreparedStatement>> preparedStatementCache;

    private final AtomicLong queryIdSeq = new AtomicLong();
    // (id + query + params) -> start nanos
    private final Map<String, Long> currentQueries = Maps.newConcurrentMap();

    private final ThreadLocal<List<Object>> currentCacheKey = new ThreadLocal<>();
    private final AtomicBoolean cacheEvictLogged = new AtomicBoolean();
    private final AtomicBoolean cacheEvictThrown = new AtomicBoolean();

    CassandraSession(Session delegate, CassandraCluster cluster) {
        this.delegate = delegate;
        this.cluster = cluster;

        int preparedStatementCacheSize = cluster.getPreparedStatementCacheSize();
        CacheBuilder<List<Object>, Async<PreparedStatement>> cacheBuilder = CacheBuilder.newBuilder()
                .maximumSize(preparedStatementCacheSize)
                .recordStats()
                .removalListener(notification -> {
                    if (notification.getCause() == RemovalCause.SIZE) {
                        String message = "Evicting cached statement due to size overlimit. " +
                                "Check for cache size (" + preparedStatementCacheSize + ") and correct parameters binding. " +
                                "This behaviour may reduce statement execution speed and cause resource overuse. " +
                                "old key " + notification.getKey() + ", new key " + currentCacheKey.get();
                        if (cacheEvictLogged.compareAndSet(false, true)) {
                            logger.error(message);
                        } else {
                            logger.warn(message);
                        }
                    }
                });
        Long expireAfterWriteSec = cluster.getExpireAfterWriteSec();
        if (expireAfterWriteSec != null) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expireAfterWriteSec, TimeUnit.SECONDS);
        }
        this.preparedStatementCache = cacheBuilder.build();
    }

    @Override
    public String getLoggedKeyspace() {
        return delegate.getLoggedKeyspace();
    }

    @Override
    public Session init() {
        return delegate.init();
    }

    @Override
    protected ResultSetAsync executeAsync(Statement statement, Object[] args) {
        long startNanos = System.nanoTime();
        String query = CassandraUtils.getQuery(statement);
        String currentQueryKey = query + " args=" + Arrays.toString(args) + " id=" + queryIdSeq.incrementAndGet();

        ResultSetFuture future;
        currentQueries.put(currentQueryKey, startNanos);
        try {
            future = delegate.executeAsync(statement);
        } catch (Throwable t) {
            currentQueries.remove(currentQueryKey);
            cqlStat.registerExecute(query, args, System.nanoTime() - startNanos, t);
            throw new RuntimeException("Error while executing statement query=[" + query + "] args=" + Arrays.toString(args), t);
        }
        Executor executor = cluster.getExecutor();
        Futures.addCallback(future, (CompleteCallback<ResultSet>) (result, exception) -> {
            currentQueries.remove(currentQueryKey);
            if (statement instanceof BatchStatement) {
                int size = ((BatchStatement) statement).size();
                cqlStat.registerBatchSize(query, size);
            }
            cqlStat.registerExecute(query, args, System.nanoTime() - startNanos, exception);
        }, executor);
        return new ResultSetAsync(future, query, args, executor).withFallback(t -> {
            throw new RuntimeException("Error while executing statement query=[" + query + "] args=" + Arrays.toString(args), t);
        }, executor);
    }

    /**
     * Overrides delegate.prepareAsync with caching of PreparedStatement object
     *
     * @param query
     * @return
     */
    @Override
    public Async<PreparedStatement> prepareAsync(String query) {
        List<Object> key = Arrays.asList(query);
        return getPreparedStatementAsync(query, key, () -> delegate.prepareAsync(query));
    }

    /**
     * Overrides delegate.prepareAsync with caching of PreparedStatement object
     *
     * @param st
     * @return
     */
    @Override
    public Async<PreparedStatement> prepareAsync(RegularStatement st) {
        String query = st.getQueryString();
        // to understand this, check out the
        // com.datastax.driver.core.AbstractSession#prepareAsync(RegularStatement)
        List<Object> key = Arrays.asList(
                query,
                st.getRoutingKey(),
                st.getConsistencyLevel(),
                st.isTracing(),
                st.getRetryPolicy()
        );
        return getPreparedStatementAsync(query, key, () -> delegate.prepareAsync(st));
    }

    private Async<PreparedStatement> getPreparedStatementAsync(String query,
                                                               List<Object> key,
                                                               Supplier<ListenableFuture<PreparedStatement>> valueSupplier) {
        Executor executor = cluster.getExecutor();
        Callable<Async<PreparedStatement>> valueLoader = () -> {
            try {
                ListenableFuture<PreparedStatement> future = valueSupplier.get();
                Futures.addCallback(future, (CompleteCallback<PreparedStatement>) (result, exception) -> {
                    cqlStat.registerPrepare(query, exception);
                }, executor);
                return Async.of(future, executor).withFallback(t -> {
                    logger.error("Error while preparing statement [retry once now] " + key, t);
                    ListenableFuture<PreparedStatement> future2 = valueSupplier.get();
                    Futures.addCallback(future2, (CompleteCallback<PreparedStatement>) (result, exception) -> {
                        cqlStat.registerPrepare(query, exception);
                    }, executor);
                    return future2;
                }).withFallback(t -> {
                    throw new RuntimeException("Error while preparing statement [async] " + key, t);
                });
            } catch (Throwable e) {
                throw new RuntimeException("Error while preparing statement [sync] " + key, e);
            }
        };

        // we can use cache of Async(ListenableFuture) because guava implementation clears all fired listeners
        currentCacheKey.set(key);
        try {
            Async<PreparedStatement> future = preparedStatementCache.get(key, valueLoader);
            Throwable failure = future.cause();
            if (failure != null) {
                preparedStatementCache.invalidate(key);
                logger.error("Cached statement " + key + " is failed, evicting and retry", failure);
                return preparedStatementCache.get(key, valueLoader);
            }

            if (cacheEvictLogged.get() && cacheEvictThrown.compareAndSet(false, true)) {
                throw new IllegalStateException("The statement " + key + " replaced evicted item, check ERROR log messages. " +
                        "All next warnings will be suppressed");
            }

            return future;
        } catch (ExecutionException e) {
            throw new RuntimeException("Error while processing prepareAsync [" + key + "]", e);
        } finally {
            currentCacheKey.remove();
        }
    }

    @Override
    public CloseFuture closeAsync() {
        return delegate.closeAsync();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public CassandraCluster getCluster() {
        return cluster;
    }

    @Override
    public State getState() {
        return delegate.getState();
    }

    @Override
    public List<Map<String, Object>> getReport(Map<String, String> args) {
        List<Map<String, Object>> res = Lists.newArrayList();

        res.add(ImmutableMap.of(
                "keySpace", getLoggedKeyspace(),
                "clusterName", cluster.getClusterName()
        ));

        Configuration config = cluster.getConfiguration();
        Policies policies = config.getPolicies();
        ReconnectionPolicy reconnectionPolicy = policies.getReconnectionPolicy();
        RetryPolicy retryPolicy = policies.getRetryPolicy();
        LoadBalancingPolicy loadBalancingPolicy = policies.getLoadBalancingPolicy();
        AddressTranslater addressTranslater = policies.getAddressTranslater();
        // unsupported
        // TimestampGenerator timestampGenerator = policies.getTimestampGenerator();

        res.add(ImmutableMap.of("config", ImmutableMap.of("policy", ImmutableMap.builder()
                .put("reconnectionPolicy", String.valueOf(reconnectionPolicy))
                .put("retryPolicy", String.valueOf(retryPolicy))
                .put("loadBalancingPolicy", String.valueOf(loadBalancingPolicy))
                .put("addressTranslater", String.valueOf(addressTranslater))
                .build())));
        if (reconnectionPolicy instanceof ReportProvider) {
            res.add(ImmutableMap.of("reconnectionPolicy", ((ReportProvider) reconnectionPolicy).getReport(args)));
        }
        if (retryPolicy instanceof ReportProvider) {
            res.add(ImmutableMap.of("retryPolicy", ((ReportProvider) retryPolicy).getReport(args)));
        }
        if (loadBalancingPolicy instanceof ReportProvider) {
            res.add(ImmutableMap.of("loadBalancingPolicy", ((ReportProvider) loadBalancingPolicy).getReport(args)));
        }
        if (addressTranslater instanceof ReportProvider) {
            res.add(ImmutableMap.of("addressTranslater", ((ReportProvider) addressTranslater).getReport(args)));
        }

        Map<String, Object> stateMap = Maps.newLinkedHashMap();
        State state = getState();
        for (Host host : state.getConnectedHosts()) {
            Map<String, Object> hostMap = Maps.newLinkedHashMap();
            hostMap.put("connections", state.getOpenConnections(host));
            hostMap.put("inFlightQueries", state.getInFlightQueries(host));
            stateMap.put(host.toString(), hostMap);
        }
        res.add(stateMap);

        long nowNanos = System.nanoTime();
        List<Map<String, Object>> currentQueriesList = Lists.newArrayList();
        for (Map.Entry<String, Long> entry : currentQueries.entrySet()) {
            String query = entry.getKey();
            long startNanos = entry.getValue();
            Map<String, Object> queryMap = ImmutableMap.of(
                    "query", query,
                    "timeoutMicros", (nowNanos - startNanos) / 1000
            );
            currentQueriesList.add(queryMap);
        }
        res.add(ImmutableMap.of("currentQueries", currentQueriesList));

        CacheStats cs = preparedStatementCache.stats();
        res.add(ImmutableMap.of("preparedStatementCacheStats", ImmutableMap.builder()
                .put("cacheSize", preparedStatementCache.size())
                .put("requestCount", cs.requestCount())
                .put("hitCount", cs.hitCount())
                .put("missCount", cs.missCount())
                .put("loadCount", cs.loadCount())
                .put("loadSuccessCount", cs.loadSuccessCount())
                .put("loadExceptionCount", cs.loadExceptionCount())
                .put("totalLoadTimeMs", TimeUnit.NANOSECONDS.toMillis(cs.totalLoadTime()))
                .put("evictionCount", cs.evictionCount())
                .put("averageLoadPenaltyMs", String.format("%.2f", cs.averageLoadPenalty() * NANOS_TO_MILLIS_MULTIPLIER))
                .build()
        ));

        res.addAll(cqlStat.report(args.get("cql")));
        return res;
    }

    public void clearCqlReport() {
        cqlStat.clear();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    /**
     * Multiplier constant to convert nanos to millis
     */
    private static final double NANOS_TO_MILLIS_MULTIPLIER = 0.000001d;
}
