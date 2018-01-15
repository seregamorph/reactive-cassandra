package com.reactivecassandra.async;

import com.google.common.annotations.Beta;

@Beta
@FunctionalInterface
public interface Function3<T1, T2, T3, R> {
    R apply(T1 r1, T2 r2, T3 r3);
}
