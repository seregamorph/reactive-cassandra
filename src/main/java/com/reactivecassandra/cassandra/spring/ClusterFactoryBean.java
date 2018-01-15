package com.reactivecassandra.cassandra.spring;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.reactivecassandra.cassandra.CassandraCluster;
import com.reactivecassandra.cassandra.ReconnectionPolicyWithStat;
import com.reactivecassandra.cassandra.RetryPolicyWithStat;
import org.springframework.beans.factory.FactoryBean;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class ClusterFactoryBean implements FactoryBean<CassandraCluster> {
    /**
     * cqlsh port
     */
    private static final int CASSANDRA_THRIFT_PORT = 9160;
    /**
     * datastax driver port
     */
    private static final int CASSANDRA_CQL_PORT = 9042;

    private static final int DEFAULT_PREPARED_STATEMENT_CACHE_SIZE = 256;
    private static final long DEFAULT_PS_EXPIRE_AFTER_WRITE_SEC = TimeUnit.MINUTES.toSeconds(10);

    private String cassandraEnsemble;
    private int cassandraPort = CASSANDRA_CQL_PORT;
    private int preparedStatementCacheSize = DEFAULT_PREPARED_STATEMENT_CACHE_SIZE;
    private long preparedStatementCacheExpireAfterWriteSec = DEFAULT_PS_EXPIRE_AFTER_WRITE_SEC;
    private Executor executor = MoreExecutors.sameThreadExecutor();
    private SocketOptions socketOptions;
    private ReconnectionPolicy reconnectionPolicy;
    private RetryPolicy retryPolicy;

    public void setCassandraEnsemble(String cassandraEnsemble) {
        this.cassandraEnsemble = cassandraEnsemble;
    }

    public void setCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
    }

    public void setPreparedStatementCacheSize(int preparedStatementCacheSize) {
        this.preparedStatementCacheSize = preparedStatementCacheSize;
    }

    /**
     * @param preparedStatementCacheExpireAfterWriteSec zero value to disable expiration
     */
    public void setPreparedStatementCacheExpireAfterWriteSec(long preparedStatementCacheExpireAfterWriteSec) {
        this.preparedStatementCacheExpireAfterWriteSec = preparedStatementCacheExpireAfterWriteSec;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public void setSocketOptions(SocketOptions socketOptions) {
        this.socketOptions = socketOptions;
    }

    public void setReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
        this.reconnectionPolicy = reconnectionPolicy;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    protected ReconnectionPolicy getReconnectionPolicy() {
        ReconnectionPolicy reconnectionPolicy = this.reconnectionPolicy;
        if (reconnectionPolicy == null) {
            reconnectionPolicy = Policies.defaultReconnectionPolicy();
        }
        return new ReconnectionPolicyWithStat(reconnectionPolicy);
    }

    protected RetryPolicy getRetryPolicy() {
        RetryPolicy retryPolicy = this.retryPolicy;
        if (retryPolicy == null) {
            retryPolicy = Policies.defaultRetryPolicy();
        }
        return new RetryPolicyWithStat(retryPolicy);
    }

    protected Cluster.Initializer buildCluster() {
        return Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(cassandraPort)
                .withSocketOptions(socketOptions)
                .withReconnectionPolicy(getReconnectionPolicy())
                .withRetryPolicy(getRetryPolicy())
                ;
    }

    protected String[] getContactPoints() {
        String hosts = this.cassandraEnsemble;
        Preconditions.checkState(hosts != null && !(hosts = hosts.trim()).isEmpty(), "Empty cassandraEnsemble");
        return hosts.split("\\s*,\\s*");
    }

    @Override
    public CassandraCluster getObject() throws Exception {
        Cluster.Initializer builder = buildCluster();
        return new CassandraCluster(builder, preparedStatementCacheSize, preparedStatementCacheExpireAfterWriteSec, executor);
    }

    @Override
    public Class<?> getObjectType() {
        return CassandraCluster.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public String toString() {
        return "ClusterFactoryBean{" +
                "cassandraEnsemble='" + cassandraEnsemble + '\'' +
                ", cassandraPort=" + cassandraPort +
                ", preparedStatementCacheSize=" + preparedStatementCacheSize +
                ", preparedStatementCacheExpireAfterWriteSec=" + preparedStatementCacheExpireAfterWriteSec +
                ", executor=" + executor +
                ", socketOptions=" + socketOptions +
                ", reconnectionPolicy=" + reconnectionPolicy +
                ", retryPolicy=" + retryPolicy +
                '}';
    }
}
