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
 * A generation-stamped borrow permit issued by
 * {@link GenericObjectPool#borrowObjectWithPermit()}.
 * <p>
 * Each permit identifies exactly one loan of one object from exactly one pool.
 * A permit is accepted by {@link GenericObjectPool#returnObject(BorrowPermit)}
 * only while it is the credential for the pool's current loan of the object
 * and only once. Once the object has been returned and borrowed again, the new
 * loan carries a new generation and permits issued for earlier loans are
 * stale: they can no longer return the re-borrowed object.
 * </p>
 * <p>
 * Instances are immutable and thread-safe.
 * </p>
 *
 * @param <T> Type of element pooled in the issuing pool.
 * @since 3.0.0
 */
public final class BorrowPermit<T> {

    /** The pool that issued this permit. */
    private final GenericObjectPool<T, ?> pool;

    /** The borrowed object. */
    private final T object;

    /** The generation of the loan this permit was issued for. */
    private final long generation;

    /** Whether this permit has already been used to return the object. */
    private final AtomicBoolean consumed = new AtomicBoolean();

    /**
     * Constructs a new permit. Only {@link GenericObjectPool} creates permits.
     *
     * @param pool the issuing pool.
     * @param object the borrowed object.
     * @param generation the loan generation.
     */
    BorrowPermit(final GenericObjectPool<T, ?> pool, final T object, final long generation) {
        this.pool = pool;
        this.object = object;
        this.generation = generation;
    }

    /**
     * Gets the generation of the loan this permit was issued for.
     *
     * @return the loan generation.
     */
    public long getGeneration() {
        return generation;
    }

    /**
     * Gets the borrowed object this permit refers to.
     *
     * @return the borrowed object.
     */
    public T getObject() {
        return object;
    }

    /**
     * Gets the pool that issued this permit.
     *
     * @return the issuing pool.
     */
    GenericObjectPool<T, ?> getPool() {
        return pool;
    }

    /**
     * Tests whether this permit has already been used to return its object.
     *
     * @return whether this permit has been consumed.
     */
    public boolean isConsumed() {
        return consumed.get();
    }

    /**
     * Atomically marks this permit consumed if it was not already.
     *
     * @return true if this call consumed the permit, false if it had already
     *         been consumed.
     */
    boolean tryConsume() {
        return consumed.compareAndSet(false, true);
    }
}
