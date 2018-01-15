package com.reactivecassandra.cassandra.spring;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.reactivecassandra.cassandra.CassandraCluster;
import com.reactivecassandra.cassandra.CassandraSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

import java.util.List;

public class CassandraSessionFactoryBean implements FactoryBean<CassandraSession> {
    private static final Logger logger = LoggerFactory.getLogger(CassandraSessionFactoryBean.class);

    private CassandraCluster cluster;
    private String keyspace;
    private List<String> connectQueries;

    public void setCluster(CassandraCluster cluster) {
        this.cluster = cluster;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public void setConnectQueries(String connectQueries) {
        List<String> list = Lists.newArrayList();
        try {
            for (String token : connectQueries.split(";")) {
                token = token.trim();
                if (token.isEmpty()) {
                    continue;
                }
                list.add(token);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while setConnectQueries" + connectQueries + "]", e);
        }
        logger.info("Connect queries: " + list);
        this.connectQueries = list;
    }

    @Override
    public CassandraSession getObject() throws Exception {
        String keyspace = this.keyspace;
        if (keyspace != null && !(keyspace = keyspace.trim()).isEmpty()) {
            Preconditions.checkState(connectQueries == null || connectQueries.isEmpty(),
                    "Not empty connect queries %s while keyspace %s is set", connectQueries, keyspace);
            logger.info("Connecting with keyspace {}", keyspace);
            return cluster.connect(keyspace);
        }

        if (connectQueries == null) {
            throw new IllegalStateException("connectQueries not set");
        }
        CassandraSession session = cluster.connect();
        for (String query : connectQueries) {
            session.execute(query);
        }
        return session;
    }

    @Override
    public Class<?> getObjectType() {
        return CassandraSession.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
