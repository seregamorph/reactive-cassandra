package com.reactivecassandra.async.strategy;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.reactivecassandra.async.AsyncStrategy;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Detailed debug info
 */
public class DebugAsyncStrategy extends AsyncStrategy {
    public static final DebugAsyncStrategy INSTANCE = new DebugAsyncStrategy();

    private static final Executor defaultExecutor = MoreExecutors.sameThreadExecutor();

    private final ThreadLocal<Context> currentContext = new ThreadLocal<>();

    protected DebugAsyncStrategy() {
    }

    @Override
    protected Executor defaultExecutor() {
        return defaultExecutor;
    }

    @Nullable
    private static DefaultContext defaultContext(@Nullable Context context) {
        if (context instanceof DefaultContext) {
            return (DefaultContext) context;
        }
        return null;
    }

    @Override
    protected <R> ListenableFuture<R> wrap(ListenableFuture<R> future, Context context0) {
        DefaultContext context = defaultContext(context0);
        return context == null ? future : Futures.withFallback(future, t -> {
            Throwable[] suppressed = t.getSuppressed();
            Throwable lastSuppressed;
            ContextException contextException;
            if (suppressed != null && suppressed.length > 0 &&
                    ((lastSuppressed = suppressed[suppressed.length - 1]) instanceof ContextException)) {
                contextException = (ContextException) lastSuppressed;
                if (context.stackTrace != null) {
                    contextException.setStackTrace(context.stackTrace.getStackTrace());
                }
            } else {
                contextException = context.stackTrace != null ? context.stackTrace : new ContextException();
                t.addSuppressed(contextException);
            }
            // put only non-exiting not to override with inactual values
            for (Map.Entry<String, Object> entry : context.getAttributes(true).entrySet()) {
                if (!contextException.attributes.containsKey(entry.getKey())) {
                    contextException.attributes.put(entry.getKey(), entry.getValue());
                }
            }

            if (t instanceof Error) {
                throw (Error) t;
            }
            if (t instanceof Exception) {
                throw (Exception) t;
            }
            throw new RuntimeException(t);
        }, executor(context));
    }

    @Override
    protected DefaultContext doCreateContext(@Nullable Context parentContext, @Nonnull Executor executor) {
        return new DefaultContext(executor, this, parentContext);
    }

    @Override
    protected void setAttribute(Context context, String name, Object value, boolean inherit) {
        DefaultContext defaultContext = defaultContext(context);
        if (defaultContext != null) {
            defaultContext.setAttribute(name, value, inherit);
        }
    }

    @Override
    protected Object getAttribute(Context context, String name) {
        DefaultContext defaultContext = defaultContext(context);
        return defaultContext == null ? null : defaultContext.getAttribute(name, true);
    }

    @Override
    protected Map<String, Object> getAttributes(Context context) {
        DefaultContext defaultContext = defaultContext(context);
        return defaultContext == null ? null : defaultContext.getAttributes(true);
    }

    @Nullable
    @Override
    protected Throwable getStackTrace(Context context) {
        DefaultContext defaultContext = defaultContext(context);
        return defaultContext == null ? null : defaultContext.stackTrace;
    }

    @Override
    protected <T, R> AsyncFunction<? super T, R> wrapFlatMap(AsyncFunction<? super T, R> asyncFunction, Context context) {
        return input -> {
            currentContext.set(context);
            try {
                return asyncFunction.apply(input);
            } finally {
                currentContext.remove();
            }
        };
    }

    private static class DefaultContext extends Context {

        private final DefaultContext parent;
        private final Map<String, Object> inheritableAttributes = Maps.newHashMap();
        private final Map<String, Object> attributes = Maps.newHashMap();
        private final ContextException stackTrace;

        DefaultContext(@Nonnull Executor executor, DebugAsyncStrategy strategy, @Nullable Context parent) {
            super(executor, strategy);
            this.parent = defaultContext(parent);

            Map<?, ?> mdc = MDC.getCopyOfContextMap();
            if (mdc != null) {
                mdc.forEach((k, v) -> inheritableAttributes.put("mdc." + k, v));
            }

            if (LOG_STACKTRACE) {
                this.stackTrace = new ContextException();
            } else {
                this.stackTrace = null;
            }
        }

        void setAttribute(String name, Object value, boolean inherit) {
            (inherit ? inheritableAttributes : attributes).put(name, value);
        }

        Object getAttribute(String name, boolean all) {
            Object value;
            return all && (value = attributes.get(name)) != null ? value :
                    (value = inheritableAttributes.get(name)) != null ? value :
                            parent != null ? parent.getAttribute(name, false) : null;
        }

        Map<String, Object> getAttributes(boolean all) {
            Map<String, Object> attrs = Maps.newLinkedHashMap();
            if (parent != null) {
                attrs.putAll(parent.getAttributes(false));
            }
            attrs.putAll(inheritableAttributes);
            if (all) {
                attrs.putAll(attributes);
            }
            return attrs;
        }
    }

    private static class ContextException extends Exception {
        private final Map<String, Object> attributes = Maps.newHashMap();

        private ContextException() {
        }

        @Override
        public String getMessage() {
            return attributes.toString();
        }

        @Override
        public String toString() {
            return getMessage();
        }
    }

    private static final boolean LOG_STACKTRACE;

    static {
        String logStackTraceStr = System.getProperty("async.logStackTrace");
        LOG_STACKTRACE = logStackTraceStr == null || logStackTraceStr.isEmpty() ||
                Boolean.parseBoolean(logStackTraceStr);
    }
}
