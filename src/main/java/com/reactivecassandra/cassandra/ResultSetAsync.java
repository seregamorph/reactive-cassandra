package com.reactivecassandra.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.annotations.Beta;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.reactivecassandra.async.Async;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

@Beta
public class ResultSetAsync extends Async<ResultSet> implements ResultSetFuture {
    private final String query;
    private final Object[] args;

    public ResultSetAsync(ListenableFuture<ResultSet> future, String query, Object[] args) {
        this(future, query, args, null);
    }

    public ResultSetAsync(ListenableFuture<ResultSet> future, String query, Object[] args, Executor executor) {
        super(future, executor);
        this.query = query;
        this.args = args != null ? args : EMPTY_ARGS;
    }

    public String getQuery() {
        return query;
    }

    public Object[] getArgs() {
        return args;
    }

    @Override
    public ResultSet getUninterruptibly() {
        try {
            return Uninterruptibles.getUninterruptibly(this);
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    @Override
    public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(this, timeout, unit);
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    public <R> Async<List<R>> mapAll(Supplier<? extends List<R>> listSupplier,
                                     Function<? super Row, ? extends R> rowMapper,
                                     Executor executor) {
        return map(rs -> {
            List<R> list = listSupplier.get();
            Iterables.transform(rs, rowMapper::apply).forEach(list::add);
            return list;
        }, executor);
    }

    public <R> Async<List<R>> mapAll(Supplier<? extends List<R>> listSupplier, Function<? super Row, ? extends R> rowMapper) {
        return mapAll(listSupplier, rowMapper, executor());
    }

    public <R> Async<List<R>> mapAll(Function<? super Row, ? extends R> rowMapper, Executor executor) {
        return mapAll(Lists::newArrayList, rowMapper, executor);
    }

    public <R> Async<List<R>> mapAll(Function<? super Row, ? extends R> rowMapper) {
        return mapAll(rowMapper, executor());
    }

    public <R> Async<R> mapSingle(Function<? super Row, ? extends R> rowMapper, Executor executor) {
        return map(rs -> CassandraUtils.singleOrNull(query, Iterables.transform(rs, rowMapper::apply)), executor);
    }

    public <R> Async<R> mapSingle(Function<? super Row, ? extends R> rowMapper) {
        return mapSingle(rowMapper, executor());
    }

    public <R> Async<Optional<R>> mapOptional(Function<? super Row, ? extends R> rowMapper, Executor executor) {
        return map(rs -> CassandraUtils.singleOrEmpty(query, Iterables.transform(rs, rowMapper::apply)), executor);
    }

    public <R> Async<Optional<R>> mapOptional(Function<? super Row, ? extends R> rowMapper) {
        return mapOptional(rowMapper, executor());
    }

    @Override
    protected <R> Async<R> subAsync(ListenableFuture<R> future, Executor executor) {
        return super.subAsync(future, executor)
                .setAttribute("query", this.query, true)
                .setAttribute("args", this.args, true);
    }

    @Override
    public ResultSetAsync sync() {
        return (ResultSetAsync) super.sync();
    }

    @Override
    public ResultSetAsync withFallback(FutureFallback<? extends ResultSet> fallback, Executor executor) {
        return new ResultSetAsync(super.withFallback(fallback, executor), getQuery(), getArgs(), executor);
    }

    @Override
    public ResultSetAsync withFallback(FutureFallback<? extends ResultSet> fallback) {
        return withFallback(fallback, executor());
    }

    @Override
    public ResultSetAsync setAttribute(String name, Object value, boolean inherit) {
        return (ResultSetAsync) super.setAttribute(name, value, inherit);
    }

    @Override
    public String toString() {
        return "ResultSetAsync{" +
                "query='" + query + '\'' +
                ", args=" + Arrays.toString(args) +
                "} " + super.toString();
    }

    private static final Object[] EMPTY_ARGS = new Object[0];
}
