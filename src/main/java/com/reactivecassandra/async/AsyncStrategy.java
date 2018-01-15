package com.reactivecassandra.async;

import com.google.common.annotations.Beta;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

@Beta
public abstract class AsyncStrategy {

    protected static Executor executor(Context context) {
        return context.executor();
    }

    protected abstract Executor defaultExecutor();

    protected <R> ListenableFuture<R> wrap(ListenableFuture<R> future, Context context) {
        return future;
    }

    protected final Context createContext(ListenableFuture<?> thisFuture, @Nullable Context parentContext, @Nullable Executor executor) {
        if (thisFuture instanceof Async) {
            return ((Async<?>) thisFuture).context();
        }
        if (executor == null) {
            executor = parentContext != null ? parentContext.executor() : defaultExecutor();
        }
        return doCreateContext(parentContext, executor);
    }

    protected abstract Context doCreateContext(@Nullable Context parentContext, @Nonnull Executor executor);

    protected <T> void completeCreate(Async<T> async) {
        if (SYNC_MODE) {
            async.sync();
        }
    }

    protected <T, R> AsyncFunction<? super T, R> wrapFlatMap(AsyncFunction<? super T, R> asyncFunction, Context context) {
        return asyncFunction;
    }

    protected void setAttribute(Context context, String name, Object value, boolean inherit) {
    }

    @Nullable
    protected Object getAttribute(Context context, String name) {
        return null;
    }

    protected Map<String, Object> getAttributes(Context context) {
        return Collections.emptyMap();
    }

    @Nullable
    protected Throwable getStackTrace(Context context) {
        return null;
    }

    /**
     * For debug purposes only
     */
    private static final boolean SYNC_MODE = Boolean.getBoolean("async.forceSync");

    protected static class Context {
        private final Executor executor;
        private final AsyncStrategy strategy;

        protected Context(@Nonnull Executor executor, AsyncStrategy strategy) {
            this.executor = checkNotNull(executor, "executor is null");
            this.strategy = checkNotNull(strategy, "strategy is null");
        }

        final Executor executor() {
            return executor;
        }

        final AsyncStrategy strategy() {
            return strategy;
        }
    }
}
