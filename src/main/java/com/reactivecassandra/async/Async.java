package com.reactivecassandra.async;

import com.google.common.annotations.Beta;
import com.google.common.util.concurrent.*;
import com.reactivecassandra.async.strategy.DefaultAsyncStrategy;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Guava ListenableFuture extension.
 * The executor argument is inherited to all sub-Async objects if not specified explicitly.
 *
 * @param <T>
 */
@Beta
public class Async<T> extends ForwardingListenableFuture<T> {
    private static volatile Supplier<AsyncStrategy> strategySupplier = () -> DefaultAsyncStrategy.INSTANCE;

    private final ListenableFuture<T> future;
    private final AsyncStrategy.Context context;

    protected Async(ListenableFuture<T> future) {
        this(future, null, null);
    }

    protected Async(ListenableFuture<T> future, @Nullable Executor executor) {
        this(future, null, executor);
    }

    private Async(ListenableFuture<T> future, @Nullable AsyncStrategy.Context parentContext, @Nullable Executor executor) {
        checkNotNull(future, "future is null");

        AsyncStrategy strategy = defaultStrategy();
        this.context = strategy.createContext(future, parentContext, executor);
        this.future = strategy.wrap(future, this.context);
        strategy.completeCreate(this);
    }

    public static Supplier<AsyncStrategy> setStrategySupplier(Supplier<AsyncStrategy> strategySupplier) {
        Supplier<AsyncStrategy> prevSupplier = checkNotNull(Async.strategySupplier);
        Async.strategySupplier = checkNotNull(strategySupplier, "strategySupplier");
        return prevSupplier;
    }

    protected AsyncStrategy defaultStrategy() {
        AsyncStrategy strategy = strategySupplier.get();
        return Objects.requireNonNull(strategy, "strategy is null");
    }

    public static <T> Async<T> of(ListenableFuture<T> future) {
        return new Async<>(future);
    }

    public static <T> Async<T> of(ListenableFuture<T> future, @Nullable Executor executor) {
        return new Async<>(future, executor);
    }

    /**
     * Create Async as immediate resolved ListenableFuture
     *
     * @param value
     * @param <T>
     * @return
     */
    public static <T> Async<T> immediate(T value) {
        return immediate(value, null);
    }

    /**
     * Create Async as immediate resolved ListenableFuture
     *
     * @param value
     * @param <T>
     * @return
     */
    public static <T> Async<T> immediate(T value, @Nullable Executor executor) {
        return of(Futures.immediateFuture(value), executor);
    }

    /**
     * Create Async as immediate resolved ListenableFuture of null
     *
     * @param <T>
     * @return
     */
    public static <T> Async<T> empty() {
        return immediate(null);
    }

    /**
     * Create Async as immediate resolved ListenableFuture of null
     *
     * @param <T>
     * @return
     */
    public static <T> Async<T> empty(@Nullable Executor executor) {
        return immediate(null, executor);
    }

    public static <T> Async<T> immediateFailed(Throwable t) {
        return immediateFailed(t, null);
    }

    public static <T> Async<T> immediateFailed(Throwable t, @Nullable Executor executor) {
        return of(Futures.immediateFailedFuture(t), executor);
    }

    /**
     * Get failure exception
     *
     * @return exception if future is done and failed, else null
     */
    @Nullable
    public Throwable cause() {
        if (!isDone()) {
            return null;
        }

        try {
            Uninterruptibles.getUninterruptibly(this);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        return null;
    }

    /**
     * returns true if future is done and failed, else false
     *
     * @return
     */
    public boolean isFailed() {
        return cause() != null;
    }

    @Override
    protected final ListenableFuture<T> delegate() {
        return future;
    }

    final AsyncStrategy.Context context() {
        return context;
    }

    protected final Executor executor() {
        return context().executor();
    }

    final AsyncStrategy strategy() {
        return context().strategy();
    }

    protected <R> Async<R> subAsync(ListenableFuture<R> future, @Nullable Executor executor) {
        return new Async<>(future, context(), executor);
    }

    /**
     * Map future result
     *
     * @param function
     * @param executor
     * @param <R>
     * @return
     */
    public <R> Async<R> map(Function<? super T, ? extends R> function, Executor executor) {
        return subAsync(Futures.transform(this.future, function::apply, executor), executor);
    }

    /**
     * Map future result
     *
     * @param function
     * @param <R>
     * @return
     */
    public <R> Async<R> map(Function<? super T, ? extends R> function) {
        return map(function, executor());
    }

    /**
     * Map future result to null. It can be useful to typesafe combine different generic Async objects.
     *
     * @return
     */
    public Async<Void> mapNull() {
        return map(r -> null);
    }

    /**
     * Map future result to another future
     *
     * @param asyncFunction
     * @param executor
     * @param <R>
     * @return
     */
    public <R> Async<R> flatMap(AsyncFunction<? super T, ? extends R> asyncFunction, Executor executor) {
        return subAsync(Futures.transform(this.future, strategy().wrapFlatMap(asyncFunction, context), executor), executor);
    }

    /**
     * Map future result to another future
     *
     * @param asyncFunction
     * @param <R>
     * @return
     */
    public <R> Async<R> flatMap(AsyncFunction<? super T, ? extends R> asyncFunction) {
        return flatMap(asyncFunction, executor());
    }

    /**
     * Wrap future with fallback callback
     *
     * @param fallback
     * @param executor
     * @return new wrapped future
     */
    public Async<T> withFallback(FutureFallback<? extends T> fallback, Executor executor) {
        return subAsync(Futures.withFallback(this, fallback, executor), executor);
    }

    /**
     * Wrap future with fallback callback
     *
     * @param fallback
     * @return new wrapped future
     */
    public Async<T> withFallback(FutureFallback<? extends T> fallback) {
        return withFallback(fallback, executor());
    }

    @Override
    public final T get() throws InterruptedException, ExecutionException {
        // do not change the behaviour of the method
        // do not catch ExecutionException or InterruptedException
        return delegate().get();
    }

    public T getUnchecked() {
        return Futures.getUnchecked(delegate());
    }

    @Nullable
    public T getNow() {
        return isDone() ? getUnchecked() : null;
    }

    /**
     * Waits for this future until it is done, and rethrows the cause of the failure if this future failed.
     */
    public Async<T> sync() {
        getUnchecked();
        return this;
    }

    public Async<T> setAttribute(String name, Object value, boolean inherit) {
        strategy().setAttribute(context(), name, value, inherit);
        return this;
    }

    @Nullable
    public Object getAttribute(String name) {
        return strategy().getAttribute(context(), name);
    }

    public Map<String, Object> getAttributes() {
        return strategy().getAttributes(context());
    }

    /**
     * Get the stacktrace of creation point if not disabled
     */
    @Nullable
    public Throwable getStackTrace() {
        return strategy().getStackTrace(context());
    }

    @Override
    public String toString() {
        return "Async{" +
                "isDone=" + isDone() +
                ", isCancelled=" + isCancelled() +
                ", context=" + context +
                '}';
    }
}
