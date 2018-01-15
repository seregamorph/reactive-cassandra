package com.reactivecassandra.async;

import com.google.common.annotations.Beta;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

@Beta
public class Asyncs {
    private Asyncs() {
    }

    @SafeVarargs
    public static <T> Async<List<T>> all(ListenableFuture<? extends T>... futures) {
        return all(Arrays.asList(futures));
    }

    public static <T> Async<List<T>> all(Iterable<? extends ListenableFuture<? extends T>> futures) {
        return Async.of(Futures.allAsList(futures));
    }

    public static <T1, T2, R> Async<R> mapAll(ListenableFuture<? extends T1> future1,
                                              ListenableFuture<? extends T2> future2,
                                              BiFunction<? super T1, ? super T2, R> function) {
        return Async.of(Futures.transform(future1, (T1 r1) -> {
            return Futures.transform(future2, (T2 r2) -> {
                return function.apply(r1, r2);
            });
        }));
    }

    public static <T1, T2, T3, R> Async<R> mapAll(ListenableFuture<? extends T1> future1,
                                                  ListenableFuture<? extends T2> future2,
                                                  ListenableFuture<? extends T3> future3,
                                                  Function3<? super T1, ? super T2, ? super T3, ? extends R> function) {
        return Async.of(Futures.transform(future1, (T1 r1) -> {
            return Futures.transform(future2, (T2 r2) -> {
                return Futures.transform(future3, (T3 r3) -> {
                    return function.apply(r1, r2, r3);
                });
            });
        }));
    }

    public static <T> CompletionStage<T> toCompletionStage(ListenableFuture<? extends T> async) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Futures.addCallback(async, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                future.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }
}
