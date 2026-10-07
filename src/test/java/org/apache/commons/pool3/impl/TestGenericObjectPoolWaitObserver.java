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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.pool3.BasePooledObjectFactory;
import org.apache.commons.pool3.ObjectPool;
import org.apache.commons.pool3.PooledObject;
import org.apache.commons.pool3.WaitEndReason;
import org.apache.commons.pool3.WaitObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests {@link WaitObserver} notifications on {@link GenericObjectPool} borrow
 * waits, plus a short regression example of the original {@link ObjectPool}
 * interface without an observer.
 * <p>
 * All synchronization uses latches and a fake clock; no test sleeps to
 * simulate waiting.
 * </p>
 */
class TestGenericObjectPoolWaitObserver {

    /**
     * A mutable fake clock used to stamp recorded events without relying on
     * wall-clock time.
     */
    static final class FakeClock extends Clock {

        private Instant current;

        FakeClock(final Instant start) {
            this.current = start;
        }

        Instant advance(final Duration duration) {
            current = current.plus(duration);
            return current;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Instant instant() {
            return current;
        }

        @Override
        public Clock withZone(final ZoneId zone) {
            return this;
        }
    }

    /** One recorded wait-observer event. */
    record WaitEvent(String kind, WaitEndReason reason, Instant stampedAt) {
        @Override
        public String toString() {
            return kind + (reason == null ? "" : "(" + reason + ")") + "@" + stampedAt;
        }
    }

    /**
     * In-memory fixture: records start/end events, stamps them with a fake
     * clock, and signals through a latch when a wait starts so tests never
     * sleep.
     */
    static final class RecordingWaitObserver implements WaitObserver {

        final FakeClock clock;
        final List<WaitEvent> events = new CopyOnWriteArrayList<>();
        final CountDownLatch waitStarted = new CountDownLatch(1);

        RecordingWaitObserver(final FakeClock clock) {
            this.clock = clock;
        }

        @Override
        public void onWaitEnd(final WaitEndReason reason, final Duration waitDuration) {
            events.add(new WaitEvent("END", reason, clock.instant()));
        }

        @Override
        public void onWaitStart(final Instant startInstant, final Duration maxWaitDuration) {
            events.add(new WaitEvent("START", null, clock.instant()));
            waitStarted.countDown();
        }
    }

    /** Simple factory creating distinct strings. */
    static final class StringFactory extends BasePooledObjectFactory<String, Exception> {

        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public String create() {
            return "obj-" + counter.incrementAndGet();
        }

        @Override
        public PooledObject<String> wrap(final String obj) {
            return new DefaultPooledObject<>(obj);
        }
    }

    /** A capacity-1 pool with blocking borrows. */
    private GenericObjectPool<String, Exception> pool;

    private void assertEvents(final RecordingWaitObserver observer, final WaitEndReason expectedReason) {
        assertEquals(2, observer.events.size(), () -> "events=" + observer.events);
        assertEquals("START", observer.events.get(0).kind());
        assertNull(observer.events.get(0).reason());
        assertEquals("END", observer.events.get(1).kind());
        assertEquals(expectedReason, observer.events.get(1).reason());
        System.out.println("[wait-events] " + observer.events);
    }

    @BeforeEach
    void setUp() {
        final GenericObjectPoolConfig<String> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1);
        config.setMaxIdle(1);
        config.setBlockWhenExhausted(true);
        pool = new GenericObjectPool<>(new StringFactory(), config);
    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    /**
     * Capacity-1 pool: a second borrower waits on a latch-controlled handoff,
     * then acquires the returned object. The wait produces exactly one
     * START/END(ACQUIRED) pair and the active count returns to zero.
     */
    @Test
    void testWaitThenAcquirePairsEvents(@TempDir final Path tempDir) throws Exception {
        final RecordingWaitObserver observer = new RecordingWaitObserver(new FakeClock(Instant.parse("2026-10-08T00:00:00Z")));
        pool.setWaitObserver(observer);

        final String held = pool.borrowObject();
        assertEquals(1, pool.getNumActive());

        final AtomicReference<Object> borrowed = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread borrower = new Thread(() -> {
            try {
                borrowed.set(pool.borrowObject(Duration.ofSeconds(30)));
            } catch (final Exception e) {
                failure.set(e);
            }
        });
        borrower.start();

        // Deterministic handoff: wait until the borrower is parked, then
        // return the object. No sleeping involved.
        assertTrue(observer.waitStarted.await(10, TimeUnit.SECONDS));
        observer.clock.advance(Duration.ofMillis(250));
        pool.returnObject(held);
        borrower.join(10_000);

        assertNull(failure.get());
        assertSame(held, borrowed.get());
        assertEquals(1, pool.getNumActive());
        assertEvents(observer, WaitEndReason.ACQUIRED);

        pool.returnObject((String) borrowed.get());
        assertEquals(0, pool.getNumActive());
        assertEquals(1, pool.getNumIdle());

        // Temp-folder fixture: persist the observed event pair for inspection.
        final Path report = tempDir.resolve("wait-events.txt");
        Files.write(report, observer.events.stream().map(WaitEvent::toString).collect(Collectors.toList()));
        assertEquals(2, Files.readAllLines(report).size());
        System.out.println("[report] " + report);
    }

    /**
     * Interrupting a waiting borrower ends the wait exactly once with
     * INTERRUPTED and keeps the original exception type.
     */
    @Test
    void testWaitInterruptedPairsEvents() throws Exception {
        final RecordingWaitObserver observer = new RecordingWaitObserver(new FakeClock(Instant.parse("2026-10-08T00:00:00Z")));
        pool.setWaitObserver(observer);

        final String held = pool.borrowObject();

        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread borrower = new Thread(() -> {
            try {
                pool.borrowObject(Duration.ofSeconds(30));
            } catch (final Exception e) {
                failure.set(e);
            }
        });
        borrower.start();

        assertTrue(observer.waitStarted.await(10, TimeUnit.SECONDS));
        observer.clock.advance(Duration.ofMillis(500));
        borrower.interrupt();
        borrower.join(10_000);

        // Exception type is unchanged: the pool rethrows the interrupt.
        assertInstanceOf(InterruptedException.class, failure.get());
        assertEvents(observer, WaitEndReason.INTERRUPTED);

        // The interrupted borrower consumed no borrow capacity.
        assertEquals(1, pool.getNumActive());
        pool.returnObject(held);
        assertEquals(0, pool.getNumActive());
        assertEquals(1, pool.getNumIdle());
    }

    /**
     * A zero wait budget on an exhausted pool times out immediately (no real
     * waiting); events are stamped with a fake clock and pair up as
     * START/END(TIMED_OUT).
     */
    @Test
    void testWaitTimeoutWithFakeTime() throws Exception {
        final FakeClock clock = new FakeClock(Instant.parse("2026-10-08T00:00:00Z"));
        final RecordingWaitObserver observer = new RecordingWaitObserver(clock);
        pool.setWaitObserver(observer);

        final String held = pool.borrowObject();
        clock.advance(Duration.ofSeconds(1));

        final NoSuchElementException e = assertThrows(NoSuchElementException.class, () -> pool.borrowObject(Duration.ZERO));
        assertTrue(e.getMessage().contains("Timeout waiting for idle object"), e.getMessage());
        assertEvents(observer, WaitEndReason.TIMED_OUT);

        // The timed-out borrower consumed no borrow capacity.
        assertEquals(1, pool.getNumActive());
        pool.returnObject(held);
        assertEquals(0, pool.getNumActive());
        assertEquals(1, pool.getNumIdle());
    }

    /**
     * An observer that throws from both callbacks must not consume borrow
     * capacity: the borrow still completes and the failures are swallowed.
     */
    @Test
    void testObserverFailureDoesNotConsumeBorrowQuota() throws Exception {
        final CountDownLatch waitStarted = new CountDownLatch(1);
        final WaitObserver failing = new WaitObserver() {
            @Override
            public void onWaitEnd(final WaitEndReason reason, final Duration waitDuration) {
                throw new IllegalStateException("boom-end");
            }

            @Override
            public void onWaitStart(final Instant startInstant, final Duration maxWaitDuration) {
                waitStarted.countDown();
                throw new IllegalStateException("boom-start");
            }
        };
        pool.setWaitObserver(failing);
        final AtomicInteger swallowed = new AtomicInteger();
        pool.setSwallowedExceptionListener(e -> swallowed.incrementAndGet());

        final String held = pool.borrowObject();

        final AtomicReference<Object> borrowed = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread borrower = new Thread(() -> {
            try {
                borrowed.set(pool.borrowObject(Duration.ofSeconds(30)));
            } catch (final Exception e) {
                failure.set(e);
            }
        });
        borrower.start();

        assertTrue(waitStarted.await(10, TimeUnit.SECONDS));
        pool.returnObject(held);
        borrower.join(10_000);

        assertNull(failure.get());
        assertSame(held, borrowed.get());
        // Both callbacks failed; both failures were swallowed, none propagated.
        assertEquals(2, swallowed.get());

        pool.returnObject((String) borrowed.get());
        assertEquals(0, pool.getNumActive());
        assertEquals(1, pool.getNumIdle());
    }

    /**
     * Short regression example of the original {@link ObjectPool} interface
     * with no observer configured: borrow/return still behaves as before and
     * the final active count matches the observer scenarios above.
     */
    @Test
    void testOriginalInterfaceRegression() throws Exception {
        assertNull(pool.getWaitObserver());
        final ObjectPool<String, Exception> originalInterface = pool;

        assertEquals(0, originalInterface.getNumActive());
        final String first = originalInterface.borrowObject();
        assertEquals(1, originalInterface.getNumActive());
        originalInterface.returnObject(first);
        assertEquals(0, originalInterface.getNumActive());

        final String second = originalInterface.borrowObject();
        assertSame(first, second);
        assertEquals(1, originalInterface.getNumActive());
        originalInterface.returnObject(second);
        assertEquals(0, originalInterface.getNumActive());
        assertEquals(1, originalInterface.getNumIdle());
        System.out.println("[regression] borrow/return OK, final active=" + originalInterface.getNumActive());
    }
}
