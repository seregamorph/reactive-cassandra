package com.reactivecassandra.async;

import com.google.common.util.concurrent.AbstractListeningExecutorService;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class AsyncExecutor extends AbstractListeningExecutorService {
    private final ExecutorService delegate;

    public AsyncExecutor(ExecutorService delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(command);
    }

    @Override
    public <T> Async<T> submit(Callable<T> task) {
        return Async.of(super.submit(task), this);
    }

    @Override
    public Async<?> submit(Runnable task) {
        return Async.of(super.submit(task), this);
    }

    @Override
    public <T> Async<T> submit(Runnable task, T result) {
        return Async.of(super.submit(task, result), this);
    }
}
