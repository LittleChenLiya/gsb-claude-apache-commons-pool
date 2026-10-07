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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.pool3.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool3.BasePooledObjectFactory;
import org.apache.commons.pool3.PooledObject;
import org.apache.commons.pool3.impl.BorrowWaitObserver.WaitEndReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests {@link BorrowWaitObserver} wired into {@link GenericObjectPool#borrowObject(Duration)}.
 * All fixtures are in-memory: a capacity-1 pool, a {@link CountDownLatch} to
 * control the waiting borrower (no sleeps), and a zero-duration wait budget
 * as the fake time source for the timeout path.
 */
@Timeout(30)
class TestBorrowWaitObserver {

    /**
     * Records wait events and releases a latch when a wait starts, so tests
     * can deterministically know a borrower is blocked.
     */
    private static final class RecordingObserver implements BorrowWaitObserver {

        final List<String> events = new CopyOnWriteArrayList<>();
        final CountDownLatch waitStartedLatch = new CountDownLatch(1);

        @Override
        public void waitEnded(final WaitEndReason reason, final Instant startInstant, final Duration waited) {
            events.add("END:" + reason);
        }

        @Override
        public void waitStarted(final Instant startInstant, final Duration maxWaitDuration) {
            events.add("START");
            waitStartedLatch.countDown();
        }
    }

    private static GenericObjectPool<String, Exception> newCapacityOnePool(final BorrowWaitObserver observer) {
        final GenericObjectPoolConfig<String> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1);
        config.setMaxWait(Duration.ofSeconds(20));
        final AtomicInteger counter = new AtomicInteger();
        final GenericObjectPool<String, Exception> pool = new GenericObjectPool<>(new BasePooledObjectFactory<String, Exception>() {
            @Override
            public String create() {
                return "object-" + counter.incrementAndGet();
            }

            @Override
            public PooledObject<String> wrap(final String obj) {
                return new DefaultPooledObject<>(obj);
            }
        }, config);
        pool.setBorrowWaitObserver(observer);
        return pool;
    }

    /**
     * Regression example for the original {@code borrowObject(long)} interface:
     * it must behave exactly like {@code borrowObject(Duration)}.
     */
    @Test
    void testBorrowObjectLongMatchesDurationOverload() throws Exception {
        final RecordingObserver observer = new RecordingObserver();
        try (GenericObjectPool<String, Exception> pool = newCapacityOnePool(observer)) {
            final String first = pool.borrowObject();
            // No wait happened for the first borrow: no events.
            assertTrue(observer.events.isEmpty());

            final NoSuchElementException fromLong = assertThrows(NoSuchElementException.class, () -> pool.borrowObject(0L));
            final NoSuchElementException fromDuration = assertThrows(NoSuchElementException.class,
                    () -> pool.borrowObject(Duration.ZERO));
            // Same exception type and message from both interfaces.
            assertEquals(fromDuration.getClass(), fromLong.getClass());
            assertEquals(fromDuration.getMessage(), fromLong.getMessage());
            // Each wait produced exactly one paired START/END(TIMEOUT).
            assertEquals(List.of("START", "END:TIMEOUT", "START", "END:TIMEOUT"), observer.events);

            // A borrow that does not wait still generates no events.
            pool.returnObject(first);
            final String again = pool.borrowObject();
            assertEquals(4, observer.events.size());
            pool.returnObject(again);
            assertEquals(0, pool.getNumActive());
        }
    }

    @Test
    void testInterruptEmitsInterruptedAndPreservesExceptionType() throws Exception {
        final RecordingObserver observer = new RecordingObserver();
        try (GenericObjectPool<String, Exception> pool = newCapacityOnePool(observer)) {
            final String first = pool.borrowObject();
            assertEquals(1, pool.getNumActive());

            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final Thread waiter = new Thread(() -> {
                try {
                    pool.borrowObject();
                    failure.set(new AssertionError("borrow should have been interrupted"));
                } catch (final InterruptedException e) {
                    // Expected: the original exception type is preserved.
                } catch (final Throwable t) {
                    failure.set(t);
                }
            });
            waiter.start();
            // Latch proves the waiter is blocked; no sleeping involved.
            assertTrue(observer.waitStartedLatch.await(20, TimeUnit.SECONDS));
            waiter.interrupt();
            waiter.join(TimeUnit.SECONDS.toMillis(20));
            assertFalse(waiter.isAlive());
            assertNull(failure.get(), () -> "unexpected: " + failure.get());

            assertEquals(List.of("START", "END:INTERRUPTED"), observer.events);
            // The interrupted borrow consumed nothing: the first object is still out.
            assertEquals(1, pool.getNumActive());
            pool.returnObject(first);
            assertEquals(0, pool.getNumActive());
        }
    }

    @Test
    void testObserverFailureDoesNotConsumeBorrowQuota() throws Exception {
        final CountDownLatch waitStartedLatch = new CountDownLatch(1);
        final BorrowWaitObserver broken = new BorrowWaitObserver() {
            @Override
            public void waitEnded(final WaitEndReason reason, final Instant startInstant, final Duration waited) {
                throw new RuntimeException("observer failure on end");
            }

            @Override
            public void waitStarted(final Instant startInstant, final Duration maxWaitDuration) {
                waitStartedLatch.countDown();
                throw new RuntimeException("observer failure on start");
            }
        };
        try (GenericObjectPool<String, Exception> pool = newCapacityOnePool(broken)) {
            final String first = pool.borrowObject();

            // Timeout path: observer failure must not change the exception type.
            assertThrows(NoSuchElementException.class, () -> pool.borrowObject(Duration.ZERO));
            assertEquals(1, pool.getNumActive());

            // Acquire-after-wait path: observer failure must not fail the borrow.
            final AtomicReference<String> borrowed = new AtomicReference<>();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final Thread waiter = new Thread(() -> {
                try {
                    borrowed.set(pool.borrowObject());
                } catch (final Throwable t) {
                    failure.set(t);
                }
            });
            waiter.start();
            assertTrue(waitStartedLatch.await(20, TimeUnit.SECONDS));
            pool.returnObject(first);
            waiter.join(TimeUnit.SECONDS.toMillis(20));
            assertFalse(waiter.isAlive());
            assertNull(failure.get(), () -> "unexpected: " + failure.get());
            assertNotNull(borrowed.get());
            assertEquals(1, pool.getNumActive());

            // Quota is intact: everything can be returned and re-borrowed.
            pool.returnObject(borrowed.get());
            assertEquals(0, pool.getNumActive());
            assertEquals(1, pool.getNumIdle());
            final String onceMore = pool.borrowObject();
            assertNotNull(onceMore);
            pool.returnObject(onceMore);
            assertEquals(0, pool.getNumActive());
        }
    }

    @Test
    void testTimeoutWithZeroWaitBudgetEmitsPairedEvents() throws Exception {
        final RecordingObserver observer = new RecordingObserver();
        try (GenericObjectPool<String, Exception> pool = newCapacityOnePool(observer)) {
            final String first = pool.borrowObject();
            assertEquals(1, pool.getNumActive());

            // Fake time: a zero wait budget ends the wait deterministically,
            // with no sleeping and no dependence on the system clock.
            final NoSuchElementException e = assertThrows(NoSuchElementException.class, () -> pool.borrowObject(Duration.ZERO));
            assertTrue(e.getMessage().contains("Timeout waiting for idle object"), e::getMessage);

            assertEquals(List.of("START", "END:TIMEOUT"), observer.events);
            // The timed-out borrow consumed nothing.
            assertEquals(1, pool.getNumActive());
            pool.returnObject(first);
            assertEquals(0, pool.getNumActive());
        }
    }

    @Test
    void testWaitThenAcquireEmitsPairedEvents() throws Exception {
        final RecordingObserver observer = new RecordingObserver();
        try (GenericObjectPool<String, Exception> pool = newCapacityOnePool(observer)) {
            final String first = pool.borrowObject();
            assertEquals(1, pool.getNumActive());

            final AtomicReference<String> borrowed = new AtomicReference<>();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final Thread waiter = new Thread(() -> {
                try {
                    borrowed.set(pool.borrowObject());
                } catch (final Throwable t) {
                    failure.set(t);
                }
            });
            waiter.start();
            // Latch proves the waiter is blocked; no sleeping involved.
            assertTrue(observer.waitStartedLatch.await(20, TimeUnit.SECONDS));
            pool.returnObject(first);
            waiter.join(TimeUnit.SECONDS.toMillis(20));
            assertFalse(waiter.isAlive());
            assertNull(failure.get(), () -> "unexpected: " + failure.get());
            assertNotNull(borrowed.get());

            assertEquals(List.of("START", "END:ACQUIRED"), observer.events);
            assertEquals(1, pool.getNumActive());
            pool.returnObject(borrowed.get());
            assertEquals(0, pool.getNumActive());
        }
    }
}
