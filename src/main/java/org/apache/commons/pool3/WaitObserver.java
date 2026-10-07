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
package org.apache.commons.pool3;

import java.time.Duration;
import java.time.Instant;

/**
 * Observes the waits a pool performs in
 * {@code borrowObject} when the pool is exhausted and
 * {@code blockWhenExhausted} is {@code true}.
 * <p>
 * For each wait, {@link #onWaitStart(Instant, Duration)} is invoked before the
 * calling thread blocks, and {@link #onWaitEnd(WaitEndReason, Duration)} is
 * invoked exactly once when the wait ends, whether the borrower acquired an
 * object, timed out, or was interrupted.
 * </p>
 * <p>
 * Implementations must be thread-safe: several borrower threads may report
 * waits concurrently. Exceptions thrown by an observer are swallowed by the
 * pool (and reported to the configured {@link SwallowedExceptionListener}, if
 * any) so that a failing observer never changes the outcome of a borrow or
 * consumes borrow capacity.
 * </p>
 *
 * @since 3.0
 */
public interface WaitObserver {

    /**
     * Notifies this observer that a borrower is about to block waiting for an
     * idle object.
     *
     * @param startInstant    the instant at which the wait starts.
     * @param maxWaitDuration the maximum duration the borrower will wait.
     */
    void onWaitStart(Instant startInstant, Duration maxWaitDuration);

    /**
     * Notifies this observer that a wait reported through
     * {@link #onWaitStart(Instant, Duration)} has ended. This is invoked
     * exactly once per wait.
     *
     * @param reason       why the wait ended.
     * @param waitDuration the approximate duration of the wait.
     */
    void onWaitEnd(WaitEndReason reason, Duration waitDuration);
}
