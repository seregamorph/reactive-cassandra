package com.reactivecassandra.async;

import com.reactivecassandra.async.strategy.DebugAsyncStrategy;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public class AsyncTest {
    @Test
    public void testInherit() {
        Supplier<AsyncStrategy> strategy = Async.setStrategySupplier(() -> DebugAsyncStrategy.INSTANCE);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Async<Object> async = Async.immediate(null, executor)
                    .setAttribute("inherit", "value", true)
                    .setAttribute("noinherit", "value", false);
            Async<?> async1 = async.map(Function.identity())
                    .flatMap(o -> Async.immediate(null))
                    .sync();

            assertEquals("value", async.getAttribute("inherit"));
            assertEquals("value", async.getAttributes().get("inherit"));
            assertEquals("value", async.getAttribute("noinherit"));
            assertEquals("value", async.getAttributes().get("noinherit"));

            assertEquals("value", async1.getAttribute("inherit"));
            assertEquals("value", async1.getAttributes().get("inherit"));
            assertNull(async1.getAttribute("noinherit"));
            assertNull(async1.getAttributes().get("noinherit"));

            assertEquals(executor, async1.executor());
        } finally {
            executor.shutdown();
            Async.setStrategySupplier(strategy);
        }
    }

    @Test
    public void testFailed() throws InterruptedException {
        try {
            Async.immediateFailed(new IOException()).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IOException);
            return;
        }
        fail();
    }

    @Test
    public void testFailedChain() throws InterruptedException {
        try {
            Async.immediateFailed(new IOException()).map(Function.identity()).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IOException);
            return;
        }
        fail();
    }

    /**
     * This test hangs if contract of Async.get() method is broken
     *
     * @throws InterruptedException
     */
    @Test(timeout = 1000L)
    public void testGetContract() throws InterruptedException {
        try {
            Async.empty().flatMap(o -> Async.immediateFailed(new IOException())).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof IOException);
            return;
        }
        fail();
    }
}
