package com.reactivecassandra.cassandra;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;

import java.util.concurrent.ExecutionException;

class DriverThrowables {
    private DriverThrowables() {
    }

    static RuntimeException propagateCause(ExecutionException e) {
        Throwable cause = e.getCause();

        if (cause instanceof Error) {
            throw ((Error) cause);
        }

        // We could just rethrow e.getCause(). However, the cause of the ExecutionException has likely been
        // created on the I/O thread receiving the response. Which means that the stacktrace associated
        // with said cause will make no mention of the current thread. This is painful for say, finding
        // out which execute() statement actually raised the exception. So instead, we re-create the
        // exception.
        if (cause instanceof DriverException) {
            throw ((DriverException) cause).copy();
        } else {
            throw new DriverInternalError("Unexpected exception thrown", cause);
        }
    }
}
