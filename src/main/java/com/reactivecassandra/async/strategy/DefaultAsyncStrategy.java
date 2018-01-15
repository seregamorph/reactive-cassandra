package com.reactivecassandra.async.strategy;

import com.google.common.util.concurrent.MoreExecutors;
import com.reactivecassandra.async.AsyncStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.Executor;

public class DefaultAsyncStrategy extends AsyncStrategy {
    public static final DefaultAsyncStrategy INSTANCE = new DefaultAsyncStrategy();

    private static final Executor defaultExecutor = MoreExecutors.sameThreadExecutor();

    protected DefaultAsyncStrategy() {
    }

    @Override
    protected Executor defaultExecutor() {
        return defaultExecutor;
    }

    @Override
    protected Context doCreateContext(@Nullable Context parentContext, @Nonnull Executor executor) {
        return new SimpleContext(executor, this);
    }

    private static class SimpleContext extends Context {
        SimpleContext(@Nonnull Executor executor, DefaultAsyncStrategy strategy) {
            super(executor, strategy);
        }
    }
}
