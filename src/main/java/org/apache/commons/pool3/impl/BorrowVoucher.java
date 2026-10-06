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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A generation-stamped voucher representing one specific borrow of an object from a
 * {@link GenericObjectPool}.
 * <p>
 * A voucher is issued by {@link GenericObjectPool#borrowVoucher()} and can be presented
 * exactly once to {@link GenericObjectPool#returnVoucher(BorrowVoucher)} to return the
 * borrowed object. Each time an object is borrowed from the pool its borrow generation
 * advances, so a voucher becomes stale as soon as the same object has been returned and
 * borrowed again: stale vouchers, vouchers that have already been used, and vouchers
 * issued by another pool are all rejected by
 * {@link GenericObjectPool#returnVoucher(BorrowVoucher)}.
 * </p>
 * <p>
 * This class is thread-safe; the single-use guarantee is enforced atomically.
 * </p>
 *
 * @param <T> Type of element pooled in the issuing pool.
 * @since 3.0.0
 */
public final class BorrowVoucher<T> {

    private final GenericObjectPool<T, ?> owner;

    private final T object;

    private final long generation;

    private final AtomicBoolean consumed = new AtomicBoolean();

    /**
     * Constructs a new voucher. Only the issuing pool creates vouchers.
     *
     * @param owner The pool that issued this voucher.
     * @param object The borrowed object this voucher returns.
     * @param generation The borrow generation of the object when this voucher was issued.
     */
    BorrowVoucher(final GenericObjectPool<T, ?> owner, final T object, final long generation) {
        this.owner = owner;
        this.object = object;
        this.generation = generation;
    }

    /**
     * Gets the borrow generation of the pooled object when this voucher was issued.
     *
     * @return The borrow generation.
     */
    public long getGeneration() {
        return generation;
    }

    /**
     * Gets the borrowed object this voucher returns.
     *
     * @return The borrowed object.
     */
    public T getObject() {
        return object;
    }

    /**
     * Gets the pool that issued this voucher.
     *
     * @return The issuing pool.
     */
    GenericObjectPool<T, ?> getOwner() {
        return owner;
    }

    /**
     * Tests whether this voucher has already been used to return its object.
     *
     * @return Whether this voucher has been consumed.
     */
    public boolean isConsumed() {
        return consumed.get();
    }

    /**
     * Atomically marks this voucher as consumed.
     *
     * @return true if this call consumed the voucher, false if it was already consumed.
     */
    boolean markConsumed() {
        return consumed.compareAndSet(false, true);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("BorrowVoucher [object=");
        builder.append(object);
        builder.append(", generation=");
        builder.append(generation);
        builder.append(", consumed=");
        builder.append(consumed);
        builder.append("]");
        return builder.toString();
    }
}
