package com.reactivecassandra.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.reactivecassandra.async.Async;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

@Beta
public class CassandraUtils {
    private CassandraUtils() {
    }

    @Nonnull
    public static String getString(Row row, String name) {
        String value = row.getString(name);
        Preconditions.checkState(value != null, "Column [%s] value is null", name);
        return value;
    }

    @Nonnull
    public static UUID getUUID(Row row, String name) {
        UUID value = row.getUUID(name);
        Preconditions.checkState(value != null, "Column [%s] value is null", name);
        return value;
    }

    public static long getLong(Row row, String name) {
        Preconditions.checkState(!row.isNull(name), "Column [%s] value is null", name);
        return row.getLong(name);
    }

    @Nullable
    public static Long getLongOrNull(Row row, String name) {
        if (row.isNull(name)) {
            return null;
        }
        return row.getLong(name);
    }

    private static String formatQuery(String query,
                                      @Nullable ConsistencyLevel consistencyLevel,
                                      @Nullable ConsistencyLevel serialConsistencyLevel,
                                      @Nullable RetryPolicy retryPolicy) {
        StringBuilder sb = new StringBuilder(query);
        if (consistencyLevel != null) {
            sb.append(" consistency=").append(consistencyLevel.name());
        }
        if (serialConsistencyLevel != null) {
            sb.append(" serialConsistency=").append(serialConsistencyLevel.name());
        }
        if (retryPolicy != null) {
            sb.append(" retryPolicy=").append(retryPolicy.getClass().getSimpleName());
        }

        return sb.toString();
    }

    public static String getQuery(Statement statement) {
        if (statement instanceof RegularStatement) {
            RegularStatement st = (RegularStatement) statement;
            return formatQuery(st.getQueryString(),
                    st.getConsistencyLevel(), st.getSerialConsistencyLevel(),
                    st.getRetryPolicy());
        }

        if (statement instanceof BoundStatement) {
            PreparedStatement ps = ((BoundStatement) statement).preparedStatement();
            return formatQuery(ps.getQueryString(),
                    ps.getConsistencyLevel(), ps.getSerialConsistencyLevel(),
                    ps.getRetryPolicy());
        }

        if (statement instanceof BatchStatement) {
            Collection<Statement> statements = ((BatchStatement) statement).getStatements();
            return statements.stream().map(CassandraUtils::getQuery).collect(Collectors.toSet()).toString();
        }

        return String.valueOf(statement);
    }

    @Nullable
    public static String getQuery(ListenableFuture<? extends ResultSet> future) {
        if (future instanceof ResultSetAsync) {
            return ((ResultSetAsync) future).getQuery();
        }
        if (future instanceof Async<?>) {
            return String.valueOf(((Async<?>) future).getAttribute("query"));
        }
        return null;
    }

    public static Assignment bindIncr(String name, BindArgs args, long value) {
        Object bind = args.bind(value);
        if (bind instanceof BindMarker) {
            return QueryBuilder.incr(name, (BindMarker) bind);
        } else {
            return QueryBuilder.incr(name, (Long) bind);
        }
    }

    public static Assignment bindDecr(String name, BindArgs args, long value) {
        Object bind = args.bind(value);
        if (bind instanceof BindMarker) {
            return QueryBuilder.decr(name, (BindMarker) bind);
        } else {
            return QueryBuilder.decr(name, (Long) bind);
        }
    }

    public static Select bindLimit(Select select, BindArgs args, int limit) {
        Object bind = args.bind(limit);
        if (bind instanceof BindMarker) {
            return select.limit((BindMarker) bind);
        } else {
            return select.limit((Integer) bind);
        }
    }

    /**
     * Get single object if exists else null
     *
     * @param <T>
     * @return
     * @throws IllegalStateException if iterator contains more than one element
     */
    @Nullable
    static <T> T singleOrNull(@Nullable String query, Iterable<? extends T> objects) throws IllegalStateException {
        Iterator<? extends T> itr = objects.iterator();
        if (!itr.hasNext()) {
            return null;
        }
        T first = itr.next();
        if (!itr.hasNext()) {
            return first;
        }

        // driver bug diagnostics
        List<T> rest = Lists.newArrayList();
        try {
            Iterators.addAll(rest, itr);
        } catch (Exception e) {
        }
        throw new IllegalStateException("Unexpected more than one element. query=[" + query + "] " +
                "first=[" + first + "] rest=" + rest);
    }

    @Nonnull
    static <T> Optional<T> singleOrEmpty(@Nullable String query, @Nonnull Iterable<? extends T> objects) throws IllegalStateException {
        Iterator<? extends T> itr = objects.iterator();
        if (!itr.hasNext()) {
            return Optional.empty();
        }
        T first = itr.next();
        if (!itr.hasNext()) {
            // note: ofNullable
            return Optional.ofNullable(first);
        }

        // driver bug diagnostics
        List<T> rest = Lists.newArrayList();
        try {
            Iterators.addAll(rest, itr);
        } catch (Exception e) {
        }
        throw new IllegalStateException("Unexpected more than one element. query=[" + query + "] " +
                "first=[" + first + "] rest=" + rest);
    }
}
