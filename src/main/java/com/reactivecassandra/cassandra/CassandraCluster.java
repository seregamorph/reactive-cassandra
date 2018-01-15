package com.reactivecassandra.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;

public class CassandraCluster extends Cluster {
    private final int preparedStatementCacheSize;
    @Nullable
    private final Long expireAfterWriteSec;
    private final Executor executor;

    /**
     * Create CassandraCluster
     *
     * @param initializer
     * @param preparedStatementCacheSize
     * @param expireAfterWriteSec        zero value disables cache (as zero size cache), null value makes it endless.
     * @param executor
     */
    public CassandraCluster(Initializer initializer, int preparedStatementCacheSize, @Nullable Long expireAfterWriteSec, Executor executor) {
        super(initializer);

        Preconditions.checkArgument(preparedStatementCacheSize > 0, "Illegal preparedStatementCacheSize %s", preparedStatementCacheSize);
        Preconditions.checkArgument(expireAfterWriteSec == null || expireAfterWriteSec >= 0,
                "Illegal expireAfterWriteSec %s", expireAfterWriteSec);
        Preconditions.checkArgument(executor != null, "executor is null");

        this.preparedStatementCacheSize = preparedStatementCacheSize;
        this.expireAfterWriteSec = expireAfterWriteSec;
        this.executor = executor;
    }

    protected CassandraSession createSession(Session delegate) {
        return new CassandraSession(delegate, this);
    }

    @Override
    public CassandraSession newSession() {
        return createSession(super.newSession());
    }

    @Override
    public CassandraSession connect() {
        return createSession(super.connect());
    }

    @Override
    public CassandraSession connect(String keyspace) {
        return createSession(super.connect(keyspace));
    }

    int getPreparedStatementCacheSize() {
        return preparedStatementCacheSize;
    }

    @Nullable
    Long getExpireAfterWriteSec() {
        return expireAfterWriteSec;
    }

    Executor getExecutor() {
        return executor;
    }
}
