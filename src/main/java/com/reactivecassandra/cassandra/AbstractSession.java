package com.reactivecassandra.cassandra;

import com.datastax.driver.core.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.reactivecassandra.async.Async;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

abstract class AbstractSession implements Session {
    /**
     * For debug purposes:
     * -DdisablePreparedStatements=true
     */
    private static final boolean DISABLE_PREPARED_STATEMENTS = Boolean.getBoolean("disablePreparedStatements");

    @Override
    public ResultSet execute(String query) {
        return execute(new SimpleStatement(query));
    }

    @Override
    public ResultSet execute(String query, Object... values) {
        return execute(new SimpleStatement(query, values), values);
    }

    @Override
    public ResultSet execute(Statement statement) {
        return execute(statement, EMPTY_ARGS);
    }

    protected ResultSet execute(Statement statement, Object[] args) {
        return executeAsync(statement, args).getUninterruptibly();
    }

    @Override
    public ResultSetAsync executeAsync(String query) {
        return executeAsync(new SimpleStatement(query));
    }

    @Override
    public ResultSetAsync executeAsync(String query, Object... values) {
        return executeAsync(new SimpleStatement(query, values), values);
    }

    /**
     * Executes a statement via delegate.executeAsync, wrapping call with stats handling and diagnostic information
     *
     * @param statement
     * @return
     */
    @Override
    public ResultSetAsync executeAsync(Statement statement) {
        return executeAsync(statement, EMPTY_ARGS);
    }

    protected abstract ResultSetAsync executeAsync(Statement statement, Object[] args);

    @Override
    public PreparedStatement prepare(String query) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsync(query));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsync(statement));
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    @Override
    public abstract Async<PreparedStatement> prepareAsync(String query);

    @Override
    public abstract Async<PreparedStatement> prepareAsync(RegularStatement st);

    /**
     * Execute a QueryBuilder with BindMarkers built statement as prepared statement
     *
     * @param function as argument accepts the newly created BindArgs object
     * @return ResultSetAsync
     */
    public ResultSetAsync executeAsync(Function<BindArgs, ? extends RegularStatement> function) {
        if (DISABLE_PREPARED_STATEMENTS) {
            return executeRegularAsync(function);
        } else {
            return executePreparedAsync(function);
        }
    }

    private ResultSetAsync executeRegularAsync(Function<BindArgs, ? extends RegularStatement> function) {
        List<Object> args = Lists.newArrayList();
        RegularStatement statement = function.apply(new BindArgs() {
            @Override
            public Object bind(Object value) {
                args.add(value);
                return value;
            }

            @Override
            public List<Object> bindAll(List<?> values) {
                args.addAll(values);
                return Lists.transform(values, o -> o);
            }
        });
        return executeAsync(statement, args.toArray());
    }

    private ResultSetAsync executePreparedAsync(Function<BindArgs, ? extends RegularStatement> function) {
        List<Object> args = Lists.newArrayList();
        RegularStatement statement = function.apply(new BindArgs() {
            @Override
            public Object bind(Object value) {
                args.add(value);
                return bindMarker();
            }

            @Override
            public List<Object> bindAll(List<?> values) {
                return Arrays.asList(bind(values));
            }
        });
        return executePreparedAsync(statement, args.toArray());
    }

    /**
     * Execute a QueryBuilder with BindMarkers built statement as prepared statement
     *
     * @param function as argument accepts the newly created BindArgs object
     * @return ResultSet
     */
    public ResultSet execute(Function<BindArgs, ? extends RegularStatement> function) {
        return executeAsync(function).getUninterruptibly();
    }

    public ResultSet executePrepared(String query, Object... args) {
        return executePreparedAsync(query, args).getUninterruptibly();
    }

    public ResultSet executePrepared(RegularStatement statement, Object... args) {
        return executePreparedAsync(statement, args).getUninterruptibly();
    }

    public ResultSetAsync executePreparedAsync(String query, Object... args) {
        Preconditions.checkNotNull(args, "args are null");
        ListenableFuture<ResultSet> future = prepareAsync(query).flatMap(ps -> {
            BoundStatement st = ps.bind(args);
            return executeAsync(st, args);
        });
        return new ResultSetAsync(future, query, args);
    }

    public ResultSetAsync executePreparedAsync(RegularStatement statement, Object... args) {
        Preconditions.checkNotNull(args, "args are null");
        ListenableFuture<ResultSet> future = prepareAsync(statement).flatMap(ps -> {
            BoundStatement st = ps.bind(args);
            st.setFetchSize(statement.getFetchSize());
            return executeAsync(st, args);
        });
        return new ResultSetAsync(future, CassandraUtils.getQuery(statement), args);
    }

    @Override
    public void close() {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final Object[] EMPTY_ARGS = new Object[0];
}
