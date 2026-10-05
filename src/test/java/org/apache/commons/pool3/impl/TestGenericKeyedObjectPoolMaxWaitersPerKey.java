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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.apache.commons.pool3.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool3.DestroyMode;
import org.apache.commons.pool3.PooledObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests the explicit per-key limit on waiting borrowers
 * ({@link GenericKeyedObjectPool#setMaxWaitersPerKey(int)}).
 * <p>
 * The limit is designed so that:
 * </p>
 * <ul>
 * <li>a hot key cannot exhaust the wait ("queue") capacity of the whole pool -
 * when the wait slots for key A are full, borrowers for key B can still
 * queue;</li>
 * <li>rejection happens before any factory object creation is attempted, so
 * the factory create count always matches the objects actually borrowed;</li>
 * <li>a wait that is cancelled (timeout or interruption) releases its ticket
 * and no longer occupies a slot;</li>
 * <li>key identity follows {@code equals()} - equal keys share a sub-pool and
 * a single wait limit;</li>
 * <li>an instance returned to the pool wakes a thread waiting for the same
 * key;</li>
 * <li>a wait that times out never destroys an object that was not borrowed;</li>
 * <li>the total waiter count and the per-key waiter counts stay consistent;</li>
 * <li>the default configuration (no limit) preserves the historical
 * {@link GenericKeyedObjectPool} behavior.</li>
 * </ul>
 * <p>
 * Tests use {@link CountDownLatch} barriers plus polling of the pool waiter
 * statistics (with generous deadlines) to deterministically observe threads
 * parked in {@code borrowObject}.
 * </p>
 */
class TestGenericKeyedObjectPoolMaxWaitersPerKey {

    /**
     * Factory that counts object creations and destructions so tests can
     * verify that rejected or timed-out borrowers never trigger factory
     * activity.
     */
    private static final class CountingFactory extends BaseKeyedPooledObjectFactory<String, String, RuntimeException> {

        final AtomicInteger createCount = new AtomicInteger();

        final AtomicInteger destroyCount = new AtomicInteger();

        private final AtomicInteger idSequence = new AtomicInteger();

        @Override
        public String create(final String key) {
            createCount.incrementAndGet();
            return key + "#" + idSequence.incrementAndGet();
        }

        @Override
        public void destroyObject(final String key, final PooledObject<String> p, final DestroyMode destroyMode) {
            destroyCount.incrementAndGet();
        }

        @Override
        public PooledObject<String> wrap(final String value) {
            return new DefaultPooledObject<>(value);
        }
    }

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);

    /**
     * Barrier: waits (polling) until the condition holds, failing the test if
     * it does not hold within a generous deadline.
     *
     * @param condition the condition to await
     * @param description description used in the failure message
     */
    private static void await(final BooleanSupplier condition, final String description) {
        final Instant deadline = Instant.now().plus(AWAIT_TIMEOUT);
        while (!condition.getAsBoolean()) {
            if (Instant.now().isAfter(deadline)) {
                fail("Timed out waiting for: " + description);
            }
            try {
                Thread.sleep(10);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted waiting for: " + description);
            }
        }
    }

    private static int totalWaitersByKey(final GenericKeyedObjectPool<String, String, RuntimeException> pool) {
        return pool.getNumWaitersByKey().values().stream().mapToInt(Integer::intValue).sum();
    }

    private CountingFactory factory;

    private GenericKeyedObjectPool<String, String, RuntimeException> pool;

    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @BeforeEach
    void setUp() {
        factory = new CountingFactory();
        executor = Executors.newCachedThreadPool(r -> {
            final Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Creates a pool limited to one instance per key with the given per-key
     * wait limit. Borrowers block (up to 30 seconds) when a sub-pool is
     * exhausted so parked waiters can be observed deterministically.
     */
    private GenericKeyedObjectPool<String, String, RuntimeException> createPool(final int maxWaitersPerKey) {
        final GenericKeyedObjectPoolConfig<String> config = new GenericKeyedObjectPoolConfig<>();
        config.setMaxTotalPerKey(1);
        config.setMaxWaitersPerKey(maxWaitersPerKey);
        config.setBlockWhenExhausted(true);
        config.setMaxWait(Duration.ofSeconds(30));
        return new GenericKeyedObjectPool<>(factory, config);
    }

    /**
     * When the wait slots for hot key A are full, a borrower for key B must
     * still be able to queue, and returning an instance wakes the waiter of
     * the same key only.
     */
    @Test
    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    void testHotKeyDoesNotExhaustWaitSlotsOfOtherKeys() throws Exception {
        pool = createPool(1);
        final String objA = pool.borrowObject("A");
        final String objB = pool.borrowObject("B");

        // Park one waiter on A: fills A's single wait slot.
        final Future<String> waiterA = executor.submit(() -> pool.borrowObject("A"));
        await(() -> pool.getNumWaitersByKey().getOrDefault("A", 0) == 1, "waiter parked on A");

        // A's wait queue is full: the next borrow for A is rejected immediately.
        assertThrows(NoSuchElementException.class, () -> pool.borrowObject("A"));

        // ... but a borrower for B can still queue.
        final Future<String> waiterB = executor.submit(() -> pool.borrowObject("B"));
        await(() -> pool.getNumWaitersByKey().getOrDefault("B", 0) == 1, "waiter parked on B");

        // Total and per-key waiter statistics must agree.
        assertEquals(2, pool.getNumWaiters());
        assertEquals(2, totalWaitersByKey(pool));

        // Returning B's instance wakes B's waiter; A's waiter keeps waiting.
        pool.returnObject("B", objB);
        assertEquals(objB, waiterB.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertFalse(waiterA.isDone(), "returning B's object must not wake A's waiter");
        assertEquals(1, pool.getNumWaitersByKey().getOrDefault("A", 0));

        // Returning A's instance wakes A's waiter.
        pool.returnObject("A", objA);
        assertEquals(objA, waiterA.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        await(() -> pool.getNumWaiters() == 0, "all waiters served");

        // The rejected borrow never triggered a factory create.
        assertEquals(2, factory.createCount.get());
        assertEquals(0, factory.destroyCount.get());

        // Hand the borrowed objects back so the pool closes cleanly.
        pool.returnObject("A", objA);
        pool.returnObject("B", objB);
    }

    /**
     * A wait that is cancelled (here: times out) must release its ticket so
     * the slot becomes available for another waiter.
     */
    @Test
    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    void testCancelledWaitReleasesTicket() throws Exception {
        pool = createPool(1);
        final String objA = pool.borrowObject("A");

        // Waiter 1 occupies A's only wait slot, then times out.
        final Future<NoSuchElementException> waiter1 = executor.submit(() -> {
            try {
                pool.borrowObject("A", 300);
                return null;
            } catch (final NoSuchElementException e) {
                return e;
            }
        });
        await(() -> pool.getNumWaitersByKey().getOrDefault("A", 0) == 1, "waiter 1 parked on A");
        final NoSuchElementException timeout = waiter1.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertTrue(timeout.getMessage().contains("Timeout waiting"), () -> "unexpected: " + timeout.getMessage());

        // The cancelled wait no longer occupies a slot...
        await(() -> pool.getNumWaitersByKey().getOrDefault("A", 0) == 0, "ticket released after timeout");

        // ... so waiter 2 may queue for the same key.
        final Future<String> waiter2 = executor.submit(() -> pool.borrowObject("A"));
        await(() -> pool.getNumWaitersByKey().getOrDefault("A", 0) == 1, "waiter 2 parked on A");

        pool.returnObject("A", objA);
        assertEquals(objA, waiter2.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        pool.returnObject("A", objA);

        // The timed-out wait neither created nor destroyed an object.
        assertEquals(1, factory.createCount.get());
        assertEquals(0, factory.destroyCount.get());
    }

    /**
     * Borrowers rejected by the wait limit must be rejected before any factory
     * creation: the number of objects created by the factory always matches
     * the objects actually handed out by the pool.
     */
    @Test
    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    void testCreateCountMatchesActualBorrows() throws Exception {
        pool = createPool(1);
        final String objA = pool.borrowObject("A");
        assertEquals(1, factory.createCount.get());
        assertEquals(1, pool.getCreatedCount());

        // Park one waiter (fills the only wait slot).
        final Future<String> waiter = executor.submit(() -> pool.borrowObject("A"));
        await(() -> pool.getNumWaitersByKey().getOrDefault("A", 0) == 1, "waiter parked on A");

        // Repeatedly exceed the wait limit: every attempt is rejected before
        // factory.create.
        for (int i = 0; i < 5; i++) {
            assertThrows(NoSuchElementException.class, () -> pool.borrowObject("A"));
        }
        assertEquals(1, factory.createCount.get(), "rejected borrowers must not create objects");
        assertEquals(1, pool.getCreatedCount());
        assertEquals(0, factory.destroyCount.get());

        // The parked waiter receives exactly the instance that was borrowed.
        pool.returnObject("A", objA);
        assertSame(objA, waiter.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(1, factory.createCount.get());
        pool.returnObject("A", objA);
        assertEquals(1, pool.getNumIdle("A"));
    }

    /**
     * Keys that are equal (per {@code equals()} / {@code hashCode()}) share a
     * single sub-pool identity and therefore a single wait limit.
     */
    @Test
    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    void testEqualKeysShareIdentityAndWaitLimit() throws Exception {
        pool = createPool(1);
        final String key1 = new String("shared");
        final String key2 = new String("shared");
        assertNotSame(key1, key2);
        assertEquals(key1, key2);

        final String obj = pool.borrowObject(key1);
        assertEquals(1, pool.getKeys().size(), "equal keys must map to one sub-pool");

        // A waiter using the equal key parks on the same sub-pool.
        final Future<String> waiter = executor.submit(() -> pool.borrowObject(key2));
        await(() -> pool.getNumWaiters() == 1, "waiter parked under equal key");
        assertEquals(1, pool.getNumWaitersByKey().size(), "waiter stats must merge equal keys");

        // The shared wait limit is exhausted for both key instances.
        assertThrows(NoSuchElementException.class, () -> pool.borrowObject(key1));
        assertThrows(NoSuchElementException.class, () -> pool.borrowObject(key2));

        // Returning under either equal key wakes the waiter.
        pool.returnObject(key1, obj);
        assertSame(obj, waiter.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        pool.returnObject(key2, obj);
        assertEquals(1, factory.createCount.get());
    }

    /**
     * A borrow that times out must not destroy objects it never borrowed, and
     * an instance returned while a waiter is parked must be handed to the
     * waiter (or stay idle), never destroyed as a side effect of a timeout.
     */
    @Test
    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    void testTimeoutDoesNotDestroyUnborrowedObjects() throws Exception {
        pool = createPool(2);
        final String obj = pool.borrowObject("A");

        // Pure timeout: nothing was borrowed, nothing may be destroyed.
        final Future<NoSuchElementException> timedOut = executor.submit(() -> {
            try {
                pool.borrowObject("A", 300);
                return null;
            } catch (final NoSuchElementException e) {
                return e;
            }
        });
        await(() -> pool.getNumWaitersByKey().getOrDefault("A", 0) == 1, "waiter parked on A");
        assertTrue(timedOut.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getMessage().contains("Timeout waiting"));
        assertEquals(0, factory.destroyCount.get(), "timeout must not destroy objects");
        assertEquals(1, factory.createCount.get());
        assertEquals(1, pool.getNumActive("A"));
        assertEquals(0, pool.getNumIdle("A"));

        // Return while a waiter is parked: the waiter gets the instance.
        final Future<String> waiter = executor.submit(() -> pool.borrowObject("A", 5_000));
        await(() -> pool.getNumWaitersByKey().getOrDefault("A", 0) == 1, "second waiter parked on A");
        pool.returnObject("A", obj);
        assertSame(obj, waiter.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        assertEquals(0, factory.destroyCount.get());

        // The same instance is still usable afterwards.
        pool.returnObject("A", obj);
        assertEquals(1, pool.getNumIdle("A"));
        assertSame(obj, pool.borrowObject("A"));
        assertEquals(0, factory.destroyCount.get());
        assertEquals(1, factory.createCount.get());
        pool.returnObject("A", obj);
    }

    /**
     * A wait limit of zero means borrowers never wait: when the sub-pool is
     * exhausted and no instance can be created, the borrow fails immediately.
     */
    @Test
    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    void testZeroMaxWaitersRejectsImmediately() throws Exception {
        pool = createPool(0);
        final String obj = pool.borrowObject("A");
        // No idle instance, no capacity to create, no wait allowed.
        assertThrows(NoSuchElementException.class, () -> pool.borrowObject("A"));
        assertEquals(1, factory.createCount.get());
        // With an idle instance available, borrowing still works.
        pool.returnObject("A", obj);
        assertSame(obj, pool.borrowObject("A"));
        pool.returnObject("A", obj);
    }

    /**
     * With the default configuration there is no wait limit: any number of
     * threads may queue for the same key (historical behavior is unchanged).
     */
    @Test
    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    void testDefaultConfigurationAllowsUnlimitedWaiters() throws Exception {
        assertEquals(-1, GenericKeyedObjectPoolConfig.DEFAULT_MAX_WAITERS_PER_KEY);
        final GenericKeyedObjectPoolConfig<String> config = new GenericKeyedObjectPoolConfig<>();
        assertEquals(GenericKeyedObjectPoolConfig.DEFAULT_MAX_WAITERS_PER_KEY, config.getMaxWaitersPerKey());
        config.setMaxTotalPerKey(1);
        config.setBlockWhenExhausted(true);
        config.setMaxWait(Duration.ofSeconds(30));
        pool = new GenericKeyedObjectPool<>(factory, config);
        assertEquals(GenericKeyedObjectPoolConfig.DEFAULT_MAX_WAITERS_PER_KEY, pool.getMaxWaitersPerKey());

        final String obj = pool.borrowObject("A");
        final int waiterCount = 5;
        final CountDownLatch parked = new CountDownLatch(waiterCount);
        final CountDownLatch done = new CountDownLatch(waiterCount);
        for (int i = 0; i < waiterCount; i++) {
            executor.submit(() -> {
                try {
                    parked.countDown();
                    final String borrowed = pool.borrowObject("A");
                    pool.returnObject("A", borrowed);
                } finally {
                    done.countDown();
                }
                return null;
            });
        }
        // Barrier: all borrowers started, then all of them parked - no limit.
        assertTrue(parked.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        await(() -> pool.getNumWaiters() == waiterCount, "all " + waiterCount + " waiters parked");
        assertEquals(waiterCount, totalWaitersByKey(pool));

        // Returning the single instance lets the waiters hand it over one by
        // one until all have been served.
        pool.returnObject("A", obj);
        assertTrue(done.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "all waiters should be served");
        assertEquals(1, factory.createCount.get());
    }

    /**
     * Total and per-key waiter statistics must stay consistent while waiters
     * for different keys are parked and while they are being served.
     */
    @Test
    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    void testWaiterStatisticsAreConsistent() throws Exception {
        pool = createPool(-1);
        final String objA = pool.borrowObject("A");
        final String objB = pool.borrowObject("B");

        final Future<String> waiterA1 = executor.submit(() -> pool.borrowObject("A"));
        final Future<String> waiterA2 = executor.submit(() -> pool.borrowObject("A"));
        final Future<String> waiterB = executor.submit(() -> pool.borrowObject("B"));
        await(() -> pool.getNumWaiters() == 3, "three waiters parked");

        final Map<String, Integer> byKey = pool.getNumWaitersByKey();
        assertEquals(2, byKey.getOrDefault("A", 0));
        assertEquals(1, byKey.getOrDefault("B", 0));
        assertEquals(pool.getNumWaiters(), byKey.values().stream().mapToInt(Integer::intValue).sum());

        pool.returnObject("A", objA);
        assertEquals(objA, waiterA1.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        await(() -> pool.getNumWaiters() == 2, "one waiter on A served");
        assertEquals(pool.getNumWaiters(), totalWaitersByKey(pool));

        pool.returnObject("B", objB);
        assertEquals(objB, waiterB.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        pool.returnObject("A", objA);
        assertEquals(objA, waiterA2.get(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        await(() -> pool.getNumWaiters() == 0, "all waiters served");
        assertEquals(0, totalWaitersByKey(pool));

        pool.returnObject("A", objA);
        pool.returnObject("B", objB);
    }

    /**
     * The limit configured on the pool and via the configuration object must
     * be visible through the getters (and the MXBean contract).
     */
    @Test
    @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
    void testMaxWaitersPerKeyAccessors() {
        final GenericKeyedObjectPoolConfig<String> config = new GenericKeyedObjectPoolConfig<>();
        config.setMaxWaitersPerKey(3);
        pool = new GenericKeyedObjectPool<>(factory, config);
        assertEquals(3, pool.getMaxWaitersPerKey());
        pool.setMaxWaitersPerKey(7);
        assertEquals(7, pool.getMaxWaitersPerKey());
        pool.setMaxWaitersPerKey(-1);
        assertEquals(-1, pool.getMaxWaitersPerKey());
    }
}
