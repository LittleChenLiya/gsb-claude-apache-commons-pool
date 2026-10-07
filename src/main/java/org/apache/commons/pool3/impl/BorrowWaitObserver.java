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

import java.time.Duration;
import java.time.Instant;

/**
 * Observes the waiting phase of {@code borrowObject} when a pool is exhausted
 * and configured to {@linkplain BaseObjectPoolConfig#getBlockWhenExhausted()
 * block when exhausted}.
 * <p>
 * An observer is notified when a borrow request starts waiting for an idle
 * object ({@link #waitStarted(Instant, Duration)}) and exactly once when that
 * wait ends ({@link #waitEnded(WaitEndReason, Instant, Duration)}), whether
 * the wait ends by acquiring an object, by interruption, or by timing out.
 * Borrows that do not wait (an idle object is available, a new object is
 * created, or the pool does not block when exhausted) generate no events.
 * </p>
 * <p>
 * Exceptions thrown by an observer are swallowed by the pool: a failing
 * observer never changes the outcome of a borrow, never consumes borrow
 * capacity, and never alters the exception types or fairness guarantees of
 * the pool.
 * </p>
 *
 * @since 3.0.0
 */
public interface BorrowWaitObserver {

    /**
     * The reason a borrow wait ended.
     *
     * @since 3.0.0
     */
    enum WaitEndReason {

        /** The wait ended because an idle object became available. */
        ACQUIRED,

        /** The wait ended because the waiting thread was interrupted. */
        INTERRUPTED,

        /** The wait ended because the maximum wait duration elapsed. */
        TIMEOUT
    }

    /**
     * An observer that ignores all events.
     */
    BorrowWaitObserver NOOP = new BorrowWaitObserver() {
        // No-op.
    };

    /**
     * Called when a borrow request is about to wait for an idle object.
     *
     * @param startInstant    the instant at which the borrow request started,
     *                        not null.
     * @param maxWaitDuration the maximum wait duration for this borrow, not
     *                        null; negative means wait indefinitely.
     */
    default void waitStarted(final Instant startInstant, final Duration maxWaitDuration) {
        // No-op.
    }

    /**
     * Called exactly once when a borrow wait ends.
     *
     * @param reason       why the wait ended, not null.
     * @param startInstant the instant at which the borrow request started, not
     *                     null.
     * @param waited       the approximate time spent waiting, not null.
     */
    default void waitEnded(final WaitEndReason reason, final Instant startInstant, final Duration waited) {
        // No-op.
    }
}
