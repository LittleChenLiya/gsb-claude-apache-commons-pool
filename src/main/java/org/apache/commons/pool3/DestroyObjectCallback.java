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

/**
 * Optional callback notified when a pool destroys a pooled object on the
 * return or close path.
 * <p>
 * Implementations are registered via
 * {@link org.apache.commons.pool3.impl.BaseObjectPoolConfig#setDestroyObjectCallback(DestroyObjectCallback)}.
 * When no callback is configured (the default), pools behave exactly as
 * before and no notifications are emitted.
 * </p>
 * <p>
 * Each pooled object is reported at most once. The callback is invoked after
 * the pool has completed its destroy accounting and the factory's
 * {@code destroyObject} invocation, so exceptions thrown by the callback do
 * not affect pool statistics or prevent the destruction from completing;
 * such exceptions are swallowed and notified via the pool's
 * {@link SwallowedExceptionListener}, if any.
 * </p>
 *
 * @param <T> the type of pooled object
 * @since 3.0.0
 */
@FunctionalInterface
public interface DestroyObjectCallback<T> {

    /**
     * Notifies that the pool destroyed the given object.
     *
     * @param object the pooled object that was destroyed
     * @param destroyReason the reason the pool destroyed the object
     * @throws Exception implementations may throw, but the pool swallows any
     *         exception so that destroy processing always completes
     */
    void onDestroy(T object, DestroyReason destroyReason) throws Exception;
}
