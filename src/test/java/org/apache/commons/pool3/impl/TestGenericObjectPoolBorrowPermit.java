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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool3.BasePooledObjectFactory;
import org.apache.commons.pool3.PooledObject;
import org.junit.jupiter.api.Test;

/**
 * Tests the generation-stamped {@link BorrowPermit} borrow and return path of
 * {@link GenericObjectPool}.
 */
class TestGenericObjectPoolBorrowPermit {

    /**
     * Fake factory that creates distinct strings and counts make/destroy calls.
     */
    private static final class CountingFactory extends BasePooledObjectFactory<String, RuntimeException> {

        private final AtomicInteger makeCount = new AtomicInteger();

        private final AtomicInteger destroyCount = new AtomicInteger();

        @Override
        public String create() {
            return "object-" + makeCount.incrementAndGet();
        }

        @Override
        public void destroyObject(final PooledObject<String> p) {
            destroyCount.incrementAndGet();
        }

        @Override
        public PooledObject<String> wrap(final String obj) {
            return new DefaultPooledObject<>(obj);
        }
    }

    private static GenericObjectPool<String, RuntimeException> createPool(final CountingFactory factory) {
        final GenericObjectPoolConfig<String> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1);
        // Fail fast instead of blocking if pool capacity ever leaks.
        config.setBlockWhenExhausted(false);
        return new GenericObjectPool<>(factory, config);
    }

    /**
     * Acceptance: after returning and re-borrowing the same object, the old
     * permit is rejected and the new permit is valid. The failed return
     * destroys the object exactly once and pool capacity does not leak.
     */
    @Test
    void testStalePermitRejectedNewPermitAccepted() {
        final CountingFactory factory = new CountingFactory();
        try (GenericObjectPool<String, RuntimeException> pool = createPool(factory)) {
            final BorrowPermit<String> permit1 = pool.borrowObjectWithPermit();
            final String object = permit1.getObject();

            // The current loan permit is accepted, exactly once.
            pool.returnObject(permit1);
            assertTrue(permit1.isConsumed());
            assertEquals(0, factory.destroyCount.get());

            // Re-borrow the same object: a new loan with a new generation.
            final BorrowPermit<String> permit2 = pool.borrowObjectWithPermit();
            assertSame(object, permit2.getObject());
            assertNotEquals(permit1.getGeneration(), permit2.getGeneration());

            // The new permit is the valid credential for the current loan.
            pool.returnObject(permit2);
            assertEquals(0, factory.destroyCount.get());

            // The stale permit cannot return the re-borrowed object; the
            // validation failure destroys the object exactly once.
            assertThrows(IllegalStateException.class, () -> pool.returnObject(permit1));
            assertEquals(1, factory.destroyCount.get());

            // Replaying the stale permit does not destroy again.
            assertThrows(IllegalStateException.class, () -> pool.returnObject(permit1));
            assertEquals(1, factory.destroyCount.get());

            // Capacity did not leak: a fresh object can be borrowed and
            // returned with maxTotal = 1.
            final BorrowPermit<String> permit3 = pool.borrowObjectWithPermit();
            assertNotNull(permit3);
            pool.returnObject(permit3);
            assertEquals(1, factory.destroyCount.get());
            assertEquals(1, pool.getNumIdle());
            assertEquals(0, pool.getNumActive());
        }
    }

    /**
     * A permit is consumed by a successful return and cannot be reused.
     */
    @Test
    void testPermitAcceptedOnlyOnce() {
        final CountingFactory factory = new CountingFactory();
        try (GenericObjectPool<String, RuntimeException> pool = createPool(factory)) {
            final BorrowPermit<String> permit = pool.borrowObjectWithPermit();
            pool.returnObject(permit);
            assertEquals(0, factory.destroyCount.get());

            // Second use of the same permit is rejected and destroys the
            // pooled object exactly once.
            assertThrows(IllegalStateException.class, () -> pool.returnObject(permit));
            assertEquals(1, factory.destroyCount.get());
            assertThrows(IllegalStateException.class, () -> pool.returnObject(permit));
            assertEquals(1, factory.destroyCount.get());
        }
    }

    /**
     * A permit issued by another pool is rejected and destroys nothing in
     * either pool.
     */
    @Test
    void testForeignPermitRejected() {
        final CountingFactory factory1 = new CountingFactory();
        final CountingFactory factory2 = new CountingFactory();
        try (GenericObjectPool<String, RuntimeException> pool1 = createPool(factory1);
                GenericObjectPool<String, RuntimeException> pool2 = createPool(factory2)) {
            final BorrowPermit<String> foreignPermit = pool2.borrowObjectWithPermit();

            assertThrows(IllegalStateException.class, () -> pool1.returnObject(foreignPermit));
            assertEquals(0, factory1.destroyCount.get());
            assertEquals(0, factory2.destroyCount.get());

            // The permit remains valid for its own pool.
            pool2.returnObject(foreignPermit);
            assertEquals(0, factory2.destroyCount.get());

            // A null permit is rejected as well.
            assertThrows(IllegalStateException.class, () -> pool1.returnObject((BorrowPermit<String>) null));
            assertEquals(0, factory1.destroyCount.get());
        }
    }

    /**
     * A plain returnObject ends the permit-based loan; the permit can no
     * longer return the object.
     */
    @Test
    void testPlainReturnEndsPermitLoan() {
        final CountingFactory factory = new CountingFactory();
        try (GenericObjectPool<String, RuntimeException> pool = createPool(factory)) {
            final BorrowPermit<String> permit = pool.borrowObjectWithPermit();
            pool.returnObject(permit.getObject());
            assertEquals(0, factory.destroyCount.get());

            assertThrows(IllegalStateException.class, () -> pool.returnObject(permit));
            assertEquals(1, factory.destroyCount.get());
        }
    }

    /**
     * The plain borrowObject/returnObject path is unaffected by the permit
     * machinery.
     */
    @Test
    void testPlainBorrowReturnUnaffected() {
        final CountingFactory factory = new CountingFactory();
        try (GenericObjectPool<String, RuntimeException> pool = createPool(factory)) {
            final String object = pool.borrowObject();
            pool.returnObject(object);
            assertSame(object, pool.borrowObject());
            assertEquals(0, factory.destroyCount.get());
            assertEquals(1, factory.makeCount.get());
        }
    }
}
