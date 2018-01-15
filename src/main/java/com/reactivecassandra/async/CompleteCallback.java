package com.reactivecassandra.async;

import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;

@FunctionalInterface
public interface CompleteCallback<T> extends FutureCallback<T> {
    @Override
    default void onSuccess(@Nullable T result) {
        onComplete(result, null);
    }

    @Override
    default void onFailure(Throwable t) {
        onComplete(null, t);
    }

    void onComplete(T result, @Nullable Throwable t);
}
