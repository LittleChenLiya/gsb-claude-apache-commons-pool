/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.pool3.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool3.BasePooledObjectFactory;
import org.apache.commons.pool3.DestroyObjectCallback;
import org.apache.commons.pool3.DestroyReason;
import org.apache.commons.pool3.PooledObject;
import org.apache.commons.pool3.TestException;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link DestroyObjectCallback} destroy reason notifications of
 * {@link GenericObjectPool}.
 */
class TestDestroyObjectCallback {

    /** Records callback invocations. */
    private static final class RecordingCallback implements DestroyObjectCallback<String> {
        final List<String> objects = new CopyOnWriteArrayList<>();
        final List<DestroyReason> reasons = new CopyOnWriteArrayList<>();
        boolean throwOnNotify;

        @Override
        public void onDestroy(final String object, final DestroyReason destroyReason) throws Exception {
            objects.add(object);
            reasons.add(destroyReason);
            if (throwOnNotify) {
                throw new Exception("callback failure");
            }
        }
    }

    /** Simple factory with counters and switches. */
    private static final class TestFactory extends BasePooledObjectFactory<String, TestException> {
        final AtomicInteger makeCount = new AtomicInteger();
        final AtomicInteger destroyCount = new AtomicInteger();
        boolean validateResult = true;
        TestException destroyException;

        @Override
        public String create() {
            return "obj-" + makeCount.incrementAndGet();
        }

        @Override
        public void destroyObject(final PooledObject<String> p) throws TestException {
            destroyCount.incrementAndGet();
            if (destroyException != null) {
                throw destroyException;
            }
        }

        @Override
        public boolean validateObject(final PooledObject<String> p) {
            return validateResult;
        }

        @Override
        public PooledObject<String> wrap(final String obj) {
            return new DefaultPooledObject<>(obj);
        }
    }

    private GenericObjectPoolConfig<String> newConfig(final RecordingCallback callback) {
        final GenericObjectPoolConfig<String> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1);
        config.setMaxIdle(1);
        config.setJmxEnabled(false);
        if (callback != null) {
            config.setDestroyObjectCallback(callback);
        }
        return config;
    }

    /**
     * Return path 1: validation on return fails, expecting RETURN_VALIDATION_FAILED.
     */
    @Test
    void testReturnValidationFailedReason() throws Exception {
        final TestFactory factory = new TestFactory();
        final RecordingCallback callback = new RecordingCallback();
        final GenericObjectPoolConfig<String> config = newConfig(callback);
        config.setTestOnReturn(true);
        try (GenericObjectPool<String, TestException> pool = new GenericObjectPool<>(factory, config)) {
            final String obj = pool.borrowObject();
            factory.validateResult = false;
            pool.returnObject(obj);

            assertEquals(List.of(DestroyReason.RETURN_VALIDATION_FAILED), callback.reasons);
            assertEquals(List.of(obj), callback.objects);
            assertEquals(1, factory.destroyCount.get());
            assertEquals(1, pool.getDestroyedCount());
            assertEquals(1, pool.getCreatedCount()); // lifetime created count
            assertEquals(0, pool.getNumIdle());

            // Pool still usable: borrowing creates a fresh object
            factory.validateResult = true;
            final String next = pool.borrowObject();
            assertEquals("obj-2", next);
            pool.returnObject(next);
        }
    }

    /**
     * Return path 2: idle capacity exceeded (maxIdle=0), expecting MAX_IDLE_EXCEEDED.
     */
    @Test
    void testMaxIdleExceededReason() throws Exception {
        final TestFactory factory = new TestFactory();
        final RecordingCallback callback = new RecordingCallback();
        final GenericObjectPoolConfig<String> config = newConfig(callback);
        config.setMaxIdle(0);
        try (GenericObjectPool<String, TestException> pool = new GenericObjectPool<>(factory, config)) {
            final String obj = pool.borrowObject();
            pool.returnObject(obj);

            assertEquals(List.of(DestroyReason.MAX_IDLE_EXCEEDED), callback.reasons);
            assertEquals(List.of(obj), callback.objects);
            assertEquals(1, factory.destroyCount.get());
            assertEquals(1, pool.getDestroyedCount());
            assertEquals(1, pool.getCreatedCount()); // lifetime created count
            assertEquals(0, pool.getNumIdle());

            final String next = pool.borrowObject();
            assertEquals("obj-2", next);
            pool.returnObject(next);
            // second object also destroyed exactly once
            assertEquals(2, callback.reasons.size());
            assertEquals(DestroyReason.MAX_IDLE_EXCEEDED, callback.reasons.get(1));
        }
    }

    /**
     * Close path: idle objects destroyed on close, expecting POOL_CLOSED.
     */
    @Test
    void testPoolClosedReason() throws Exception {
        final TestFactory factory = new TestFactory();
        final RecordingCallback callback = new RecordingCallback();
        final GenericObjectPool<String, TestException> pool = new GenericObjectPool<>(factory, newConfig(callback));
        final String obj = pool.borrowObject();
        pool.returnObject(obj);
        assertEquals(1, pool.getNumIdle());

        pool.close();

        assertEquals(List.of(DestroyReason.POOL_CLOSED), callback.reasons);
        assertEquals(List.of(obj), callback.objects);
        assertEquals(1, factory.destroyCount.get());
        assertEquals(1, pool.getDestroyedCount());
        assertEquals(1, pool.getCreatedCount()); // lifetime created count
        assertEquals(0, pool.getNumIdle());
    }

    /**
     * A throwing callback must not prevent destroy accounting, and the pool
     * must remain usable afterwards.
     */
    @Test
    void testThrowingCallbackStillCompletesDestroy() throws Exception {
        final TestFactory factory = new TestFactory();
        final RecordingCallback callback = new RecordingCallback();
        callback.throwOnNotify = true;
        final GenericObjectPoolConfig<String> config = newConfig(callback);
        config.setMaxIdle(0);
        try (GenericObjectPool<String, TestException> pool = new GenericObjectPool<>(factory, config)) {
            final String obj = pool.borrowObject();
            // returnObject must not propagate the callback failure
            pool.returnObject(obj);

            assertEquals(List.of(DestroyReason.MAX_IDLE_EXCEEDED), callback.reasons);
            assertEquals(1, factory.destroyCount.get());
            assertEquals(1, pool.getDestroyedCount());
            assertEquals(1, pool.getCreatedCount()); // lifetime created count

            // Pool still usable: a new object can be borrowed
            final String next = pool.borrowObject();
            assertEquals("obj-2", next);
            pool.returnObject(next);
            assertEquals(2, factory.destroyCount.get());
            assertEquals(2, pool.getDestroyedCount());
        }
    }

    /**
     * Factory destroy exceptions keep their cause when propagating through
     * the pool (existing behavior is preserved).
     */
    @Test
    void testFactoryDestroyExceptionKeepsCause() throws Exception {
        final TestFactory factory = new TestFactory();
        final RecordingCallback callback = new RecordingCallback();
        final RuntimeException cause = new RuntimeException("root cause");
        factory.destroyException = new TestException("destroy failed", cause);
        try (GenericObjectPool<String, TestException> pool = new GenericObjectPool<>(factory, newConfig(callback))) {
            final String obj = pool.borrowObject();
            final TestException thrown = assertThrows(TestException.class, () -> pool.invalidateObject(obj));
            assertSame(cause, thrown.getCause());
            // destroy accounting still completed
            assertEquals(1, factory.destroyCount.get());
            assertEquals(1, pool.getDestroyedCount());
            assertEquals(1, pool.getCreatedCount()); // lifetime created count
        }
    }

    /**
     * Each object is notified at most once, even when several idle objects
     * are destroyed by a single close.
     */
    @Test
    void testEachObjectNotifiedOnlyOnce() throws Exception {
        final TestFactory factory = new TestFactory();
        final RecordingCallback callback = new RecordingCallback();
        final GenericObjectPoolConfig<String> config = newConfig(callback);
        config.setMaxTotal(2);
        config.setMaxIdle(2);
        final GenericObjectPool<String, TestException> pool = new GenericObjectPool<>(factory, config);
        final String obj1 = pool.borrowObject();
        final String obj2 = pool.borrowObject();
        pool.returnObject(obj1);
        pool.returnObject(obj2);

        pool.close();

        assertEquals(2, callback.reasons.size());
        assertTrue(callback.reasons.stream().allMatch(DestroyReason.POOL_CLOSED::equals));
        assertEquals(2, callback.objects.stream().distinct().count());
        assertEquals(2, factory.destroyCount.get());
        assertEquals(2, pool.getDestroyedCount());
    }

    /**
     * Without the new option, the pool behaves exactly as before: no
     * callback, normal borrow/return/close.
     */
    @Test
    void testCallbackDisabledByDefault() throws Exception {
        final TestFactory factory = new TestFactory();
        final GenericObjectPoolConfig<String> config = newConfig(null);
        config.setMaxIdle(0);
        assertNull(config.getDestroyObjectCallback());
        try (GenericObjectPool<String, TestException> pool = new GenericObjectPool<>(factory, config)) {
            assertNull(pool.getDestroyObjectCallback());
            final String obj = pool.borrowObject();
            pool.returnObject(obj);
            assertEquals(1, factory.destroyCount.get());
            assertEquals(1, pool.getDestroyedCount());
            final String next = pool.borrowObject();
            assertEquals("obj-2", next);
            pool.returnObject(next);
        }
        assertEquals(2, factory.destroyCount.get());
    }
}
