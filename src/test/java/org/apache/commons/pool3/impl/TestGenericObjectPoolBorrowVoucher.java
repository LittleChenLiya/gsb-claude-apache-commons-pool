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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool3.BasePooledObjectFactory;
import org.apache.commons.pool3.PooledObject;
import org.junit.jupiter.api.Test;

/**
 * Tests the generation-stamped borrow voucher support of {@link GenericObjectPool}:
 * {@link GenericObjectPool#borrowVoucher()} and
 * {@link GenericObjectPool#returnVoucher(BorrowVoucher)}.
 */
class TestGenericObjectPoolBorrowVoucher {

    /**
     * Fake factory creating plain objects, counting destroys and allowing
     * validation outcomes to be controlled by the test.
     */
    static class FakeFactory extends BasePooledObjectFactory<Object, RuntimeException> {

        private final AtomicInteger destroyCount = new AtomicInteger();

        private volatile boolean valid = true;

        @Override
        public Object create() {
            return new Object();
        }

        @Override
        public void destroyObject(final PooledObject<Object> p) {
            destroyCount.incrementAndGet();
        }

        int getDestroyCount() {
            return destroyCount.get();
        }

        void setValid(final boolean valid) {
            this.valid = valid;
        }

        @Override
        public boolean validateObject(final PooledObject<Object> p) {
            return valid;
        }

        @Override
        public PooledObject<Object> wrap(final Object obj) {
            return new DefaultPooledObject<>(obj);
        }
    }

    private GenericObjectPool<Object, RuntimeException> createPool(final FakeFactory factory) {
        final GenericObjectPoolConfig<Object> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1);
        return new GenericObjectPool<>(factory, config);
    }

    /**
     * A plain borrow/return cycle must keep working unchanged.
     */
    @Test
    void testPlainBorrowAndReturnStillWork() throws Exception {
        final FakeFactory factory = new FakeFactory();
        try (GenericObjectPool<Object, RuntimeException> pool = createPool(factory)) {
            final Object obj = pool.borrowObject();
            pool.returnObject(obj);
            assertEquals(1, pool.getNumIdle());
            assertEquals(0, pool.getNumActive());
            assertEquals(0, factory.getDestroyCount());
        }
    }

    /**
     * After an object is returned and the same object is borrowed again, the
     * old voucher must be rejected while the voucher for the new borrow is
     * accepted.
     */
    @Test
    void testOldVoucherRejectedAfterReturnAndReborrowOfSameObject() throws Exception {
        final FakeFactory factory = new FakeFactory();
        try (GenericObjectPool<Object, RuntimeException> pool = createPool(factory)) {
            final BorrowVoucher<Object> firstVoucher = pool.borrowVoucher();
            final Object obj = firstVoucher.getObject();

            pool.returnVoucher(firstVoucher);
            assertEquals(1, pool.getNumIdle());

            // maxTotal == 1, so the same object is borrowed again.
            final BorrowVoucher<Object> secondVoucher = pool.borrowVoucher();
            assertSame(obj, secondVoucher.getObject());
            assertTrue(secondVoucher.getGeneration() > firstVoucher.getGeneration());

            // The old voucher must not be able to return the re-borrowed object.
            assertThrows(IllegalStateException.class, () -> pool.returnVoucher(firstVoucher));
            assertEquals(1, pool.getNumActive());
            assertEquals(0, pool.getNumIdle());

            // The voucher for the current borrow is accepted.
            pool.returnVoucher(secondVoucher);
            assertEquals(0, pool.getNumActive());
            assertEquals(1, pool.getNumIdle());
            assertEquals(0, factory.getDestroyCount());
        }
    }

    /**
     * A voucher can be used to return its object exactly once.
     */
    @Test
    void testVoucherIsSingleUse() throws Exception {
        final FakeFactory factory = new FakeFactory();
        try (GenericObjectPool<Object, RuntimeException> pool = createPool(factory)) {
            final BorrowVoucher<Object> voucher = pool.borrowVoucher();
            pool.returnVoucher(voucher);
            assertTrue(voucher.isConsumed());
            assertThrows(IllegalStateException.class, () -> pool.returnVoucher(voucher));
            assertEquals(1, pool.getNumIdle());
            assertEquals(0, pool.getNumActive());
        }
    }

    /**
     * A voucher issued by one pool must be rejected by another pool.
     */
    @Test
    void testVoucherFromOtherPoolRejected() throws Exception {
        final FakeFactory factory = new FakeFactory();
        try (GenericObjectPool<Object, RuntimeException> pool = createPool(factory);
                GenericObjectPool<Object, RuntimeException> otherPool = createPool(new FakeFactory())) {
            final BorrowVoucher<Object> voucher = pool.borrowVoucher();
            assertThrows(IllegalStateException.class, () -> otherPool.returnVoucher(voucher));
            // The object is still checked out of its own pool and can be returned there.
            assertEquals(1, pool.getNumActive());
            pool.returnVoucher(voucher);
            assertEquals(1, pool.getNumIdle());
        }
    }

    /**
     * A plain {@link GenericObjectPool#borrowObject()} also advances the borrow
     * generation, so a voucher issued before a plain return/re-borrow cycle of
     * the same object is stale.
     */
    @Test
    void testVoucherStaleAfterPlainReborrow() throws Exception {
        final FakeFactory factory = new FakeFactory();
        try (GenericObjectPool<Object, RuntimeException> pool = createPool(factory)) {
            final BorrowVoucher<Object> voucher = pool.borrowVoucher();
            final Object obj = voucher.getObject();

            // Return and re-borrow the same object without vouchers.
            pool.returnObject(obj);
            assertSame(obj, pool.borrowObject());

            assertThrows(IllegalStateException.class, () -> pool.returnVoucher(voucher));
            assertEquals(1, pool.getNumActive());
            pool.returnObject(obj);
            assertEquals(1, pool.getNumIdle());
        }
    }

    /**
     * A voucher whose object has been invalidated (destroyed) is rejected.
     */
    @Test
    void testVoucherRejectedAfterInvalidate() throws Exception {
        final FakeFactory factory = new FakeFactory();
        try (GenericObjectPool<Object, RuntimeException> pool = createPool(factory)) {
            final BorrowVoucher<Object> voucher = pool.borrowVoucher();
            pool.invalidateObject(voucher.getObject());
            assertEquals(1, factory.getDestroyCount());
            assertThrows(IllegalStateException.class, () -> pool.returnVoucher(voucher));
            assertEquals(1, factory.getDestroyCount());
        }
    }

    /**
     * When testOnReturn validation fails for a voucher return, the object must
     * be destroyed exactly once through the normal destroy path and the pool
     * capacity must not leak.
     */
    @Test
    void testValidationFailureDestroysExactlyOnceWithoutCapacityLeak() throws Exception {
        final FakeFactory factory = new FakeFactory();
        final GenericObjectPoolConfig<Object> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1);
        config.setTestOnReturn(true);
        try (GenericObjectPool<Object, RuntimeException> pool = new GenericObjectPool<>(factory, config)) {
            final BorrowVoucher<Object> voucher = pool.borrowVoucher();

            factory.setValid(false);
            pool.returnVoucher(voucher);

            // Exactly one destroy, nothing left in the pool.
            assertEquals(1, factory.getDestroyCount());
            assertEquals(1, pool.getDestroyedCount());
            assertEquals(0, pool.getNumActive());
            assertEquals(0, pool.getNumIdle());

            // Capacity has not leaked: with maxTotal == 1 a new object can be
            // created and borrowed immediately.
            factory.setValid(true);
            final Object replacement = pool.borrowObject();
            assertEquals(2, pool.getCreatedCount());
            assertEquals(1, pool.getNumActive());
            pool.returnObject(replacement);
            assertEquals(1, factory.getDestroyCount());
            assertEquals(1, pool.getNumIdle());
        }
    }
}
