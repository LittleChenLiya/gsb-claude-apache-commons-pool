/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.pool3.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool3.BasePooledObjectFactory;
import org.apache.commons.pool3.DestroyReason;
import org.apache.commons.pool3.ObjectDestroyListener;
import org.apache.commons.pool3.PooledObject;
import org.apache.commons.pool3.SwallowedExceptionListener;
import org.apache.commons.pool3.TestException;
import org.junit.jupiter.api.Test;

/**
 * Tests the optional {@link ObjectDestroyListener} destruction-reason
 * callback for the three return-time destruction paths:
 * validation failure on return, {@code maxIdle} exceeded and pool closed.
 *
 * <p>All fixtures are in-memory: a counting factory and a recording
 * listener.</p>
 */
public class TestObjectDestroyListener {

    /** In-memory factory: counts makes/destroys and can fail validation or destroy. */
    static class CountingFactory extends BasePooledObjectFactory<String, TestException> {

        final AtomicInteger makeCount = new AtomicInteger();

        final AtomicInteger destroyCount = new AtomicInteger();

        volatile boolean validOnReturn = true;

        volatile boolean throwOnDestroy;

        @Override
        public String create() {
            return "object-" + makeCount.incrementAndGet();
        }

        @Override
        public void destroyObject(final PooledObject<String> p) throws TestException {
            destroyCount.incrementAndGet();
            if (throwOnDestroy) {
                throw new TestException("factory destroy failed for " + p.getObject());
            }
        }

        @Override
        public boolean validateObject(final PooledObject<String> p) {
            return validOnReturn;
        }

        @Override
        public PooledObject<String> wrap(final String value) {
            return new DefaultPooledObject<>(value);
        }
    }

    /** Recording listener: counts notifications per object and can itself throw. */
    static class RecordingListener implements ObjectDestroyListener<String> {

        final List<DestroyReason> reasons = Collections.synchronizedList(new ArrayList<>());

        final List<String> objects = Collections.synchronizedList(new ArrayList<>());

        volatile boolean throwOnNotify;

        final AtomicInteger notifyCount = new AtomicInteger();

        @Override
        public void onDestroyObject(final PooledObject<String> p, final DestroyReason reason) {
            notifyCount.incrementAndGet();
            objects.add(p.getObject());
            reasons.add(reason);
            if (throwOnNotify) {
                throw new IllegalStateException("listener failure for " + p.getObject());
            }
        }
    }

    static class RecordingSwallowedListener implements SwallowedExceptionListener {
        final List<Exception> swallowed = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onSwallowException(final Exception e) {
            swallowed.add(e);
        }
    }

    private GenericObjectPoolConfig<String> newConfig(final int maxTotal, final int maxIdle,
            final boolean testOnReturn, final ObjectDestroyListener<String> listener) {
        final GenericObjectPoolConfig<String> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(0);
        config.setBlockWhenExhausted(false);
        config.setTestOnReturn(testOnReturn);
        config.setObjectDestroyListener(listener);
        return config;
    }

    /** Path 1: object returned but validation on return fails. */
    @Test
    public void testReturnValidationFailed() throws Exception {
        final CountingFactory factory = new CountingFactory();
        factory.validOnReturn = false;
        final RecordingListener listener = new RecordingListener();
        try (GenericObjectPool<String, TestException> pool =
                new GenericObjectPool<>(factory, newConfig(1, 1, true, listener))) {

            final String obj = pool.borrowObject();
            pool.returnObject(obj);

            assertArrayEquals(new DestroyReason[] {DestroyReason.RETURN_VALIDATION_FAILED},
                    listener.reasons.toArray());
            assertEquals(1, listener.notifyCount.get(), "object notified once");
            assertEquals(1, factory.destroyCount.get(), "factory destroy count");
            assertEquals(1L, pool.getDestroyedCount(), "pool destroyedCount");
            assertEquals(1L, pool.getCreatedCount());
            assertEquals(0, pool.getNumActive());
            assertEquals(0, pool.getNumIdle());
        }
    }

    /** Path 2: object returned while idle pool is already at maxIdle (0 here). */
    @Test
    public void testMaxIdleExceeded() throws Exception {
        final CountingFactory factory = new CountingFactory();
        final RecordingListener listener = new RecordingListener();
        try (GenericObjectPool<String, TestException> pool =
                new GenericObjectPool<>(factory, newConfig(1, 0, false, listener))) {

            final String obj = pool.borrowObject();
            pool.returnObject(obj);

            assertArrayEquals(new DestroyReason[] {DestroyReason.MAX_IDLE_EXCEEDED},
                    listener.reasons.toArray());
            assertEquals(1, listener.notifyCount.get(), "object notified once");
            assertEquals(1, factory.destroyCount.get(), "factory destroy count");
            assertEquals(1L, pool.getDestroyedCount(), "pool destroyedCount");
            assertEquals(0, pool.getNumActive());
            assertEquals(0, pool.getNumIdle());
        }
    }

    /** Path 3a: closing the pool destroys the one idle object. */
    @Test
    public void testPoolClosedDestroysIdleObject() throws Exception {
        final CountingFactory factory = new CountingFactory();
        final RecordingListener listener = new RecordingListener();
        final GenericObjectPool<String, TestException> pool =
                new GenericObjectPool<>(factory, newConfig(1, 8, false, listener));

        final String obj = pool.borrowObject();
        pool.returnObject(obj);
        assertEquals(1, pool.getNumIdle());

        pool.close();

        assertArrayEquals(new DestroyReason[] {DestroyReason.POOL_CLOSED}, listener.reasons.toArray());
        assertEquals(1, listener.notifyCount.get(), "object notified once");
        assertEquals(1, factory.destroyCount.get(), "factory destroy count");
        assertEquals(1L, pool.getDestroyedCount(), "pool destroyedCount");
        assertEquals(0, pool.getNumActive());
        assertEquals(0, pool.getNumIdle());

        // close() is idempotent and must not notify or destroy again
        pool.close();
        assertEquals(1, listener.notifyCount.get(), "object still notified once after re-close");
        assertEquals(1, factory.destroyCount.get());
    }

    /** Path 3b: returning a checked-out object to an already closed pool destroys it. */
    @Test
    public void testReturnAfterPoolClosed() throws Exception {
        final CountingFactory factory = new CountingFactory();
        final RecordingListener listener = new RecordingListener();
        final GenericObjectPool<String, TestException> pool =
                new GenericObjectPool<>(factory, newConfig(1, 8, false, listener));

        final String obj = pool.borrowObject();
        pool.close();
        assertEquals(0, listener.notifyCount.get(), "active object is not destroyed by close()");

        pool.returnObject(obj);

        assertArrayEquals(new DestroyReason[] {DestroyReason.POOL_CLOSED}, listener.reasons.toArray());
        assertEquals(1, listener.notifyCount.get());
        assertEquals(1, factory.destroyCount.get());
        assertEquals(1L, pool.getDestroyedCount());
        assertEquals(0, pool.getNumActive());
        assertEquals(0, pool.getNumIdle());
    }

    /** A throwing listener must not stop destruction, counters or subsequent borrowing. */
    @Test
    public void testThrowingListenerStillDestroysAndPoolStaysUsable() throws Exception {
        final CountingFactory factory = new CountingFactory();
        final RecordingListener listener = new RecordingListener();
        listener.throwOnNotify = true;
        final RecordingSwallowedListener swallowed = new RecordingSwallowedListener();
        try (GenericObjectPool<String, TestException> pool =
                new GenericObjectPool<>(factory, newConfig(1, 0, false, listener))) {
            pool.setSwallowedExceptionListener(swallowed);

            final String first = pool.borrowObject();
            pool.returnObject(first); // maxIdle path; listener blows up

            // Callback happened, factory destroy still ran and counters moved
            assertEquals(1, listener.notifyCount.get());
            assertArrayEquals(new DestroyReason[] {DestroyReason.MAX_IDLE_EXCEEDED},
                    listener.reasons.toArray());
            assertEquals(1, factory.destroyCount.get());
            assertEquals(1L, pool.getDestroyedCount());
            assertEquals(0, pool.getNumActive());
            assertEquals(0, pool.getNumIdle());
            // Listener error surfaced through the swallowed-exception channel
            assertEquals(1, swallowed.swallowed.size());
            assertNotNull(swallowed.swallowed.get(0).getMessage());

            // Pool remains usable: a brand new object can be borrowed
            final String second = pool.borrowObject();
            assertFalse(first.equals(second), "a fresh object was created after the failure");
            assertEquals(2, factory.makeCount.get());
            assertEquals(1, factory.destroyCount.get());
            pool.returnObject(second);
            assertEquals(2, factory.destroyCount.get());
            assertEquals(2, listener.notifyCount.get(), "each object notified exactly once");
            assertEquals(2, listener.objects.size());
        }
    }

    /** Factory destroy exception keeps its cause; a listener error is attached as suppressed. */
    @Test
    public void testFactoryExceptionKeepsCauseWhenListenerAlsoFails() throws Exception {
        final CountingFactory factory = new CountingFactory();
        factory.throwOnDestroy = true;
        final RecordingListener listener = new RecordingListener();
        listener.throwOnNotify = true;
        final RecordingSwallowedListener swallowed = new RecordingSwallowedListener();
        try (GenericObjectPool<String, TestException> pool =
                new GenericObjectPool<>(factory, newConfig(1, 0, false, listener))) {
            pool.setSwallowedExceptionListener(swallowed);

            final String obj = pool.borrowObject();
            pool.returnObject(obj); // both listener and factory fail

            assertEquals(1, listener.notifyCount.get());
            assertEquals(1, factory.destroyCount.get(), "factory destroy still invoked");
            assertEquals(1L, pool.getDestroyedCount(), "counters still updated");
            assertEquals(1, swallowed.swallowed.size());
            final Exception e = swallowed.swallowed.get(0);
            assertSame(TestException.class, e.getClass(), "factory exception/cause retained");
            assertTrue(e.getMessage().contains("factory destroy failed"));
            assertEquals(1, e.getSuppressed().length, "listener failure attached as suppressed");
            assertTrue(e.getSuppressed()[0] instanceof IllegalStateException);

            // Pool stays usable despite both failures
            assertNotNull(pool.borrowObject());
        }
    }

    /** When the new option is not enabled, the original interface works unchanged. */
    @Test
    public void testDisabledByDefaultKeepsOriginalBehavior() throws Exception {
        final CountingFactory factory = new CountingFactory();
        final GenericObjectPoolConfig<String> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1);
        config.setBlockWhenExhausted(false);
        try (GenericObjectPool<String, TestException> pool = new GenericObjectPool<>(factory, config)) {
            assertNull(pool.getObjectDestroyListener());

            final String obj = pool.borrowObject();
            pool.returnObject(obj);
            assertEquals(obj, pool.borrowObject(), "object is reused, nothing destroyed");
            assertEquals(0, factory.destroyCount.get());
            assertEquals(0L, pool.getDestroyedCount());

            pool.invalidateObject(obj);
            assertEquals(1, factory.destroyCount.get(), "invalidation still works without callback");
            assertEquals(1L, pool.getDestroyedCount());
            assertNotNull(pool.borrowObject(), "pool still serves new objects");
        }
    }
}
