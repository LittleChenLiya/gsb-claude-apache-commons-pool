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
 * A listener notified once, with the {@link DestroyReason}, when a pooled
 * object is destroyed because it failed validation on return, exceeded the
 * configured {@code maxIdle} count, or because the pool was closed.
 * <p>
 * A listener is only invoked when it is explicitly configured (for example via
 * {@code GenericObjectPoolConfig.setObjectDestroyListener(ObjectDestroyListener)});
 * when no listener is configured, pools behave exactly as in previous versions.
 * </p>
 * <p>
 * Notifications are delivered before
 * {@link PooledObjectFactory#destroyObject(PooledObject, DestroyMode)} and a
 * given pooled object is notified at most once. If
 * {@link #onDestroyObject(PooledObject, DestroyReason)} throws, the exception
 * is swallowed (and forwarded to a
 * {@link SwallowedExceptionListener} when one is configured); the factory's
 * {@code destroyObject} method is still invoked and the pool counters are still
 * updated. A failure thrown by the factory's {@code destroyObject} method keeps
 * its original cause; a listener failure is attached to it as a
 * {@link Throwable#addSuppressed(Throwable) suppressed} exception in that case.
 * </p>
 *
 * @param <T> Type of element managed in the pool.
 * @see DestroyReason
 * @since 3.0.0
 */
@FunctionalInterface
public interface ObjectDestroyListener<T> {

    /**
     * Notifies that the given pooled object is about to be destroyed for the
     * given reason.
     * <p>
     * Implementations should avoid throwing. Any exception thrown by this
     * method does not prevent the object from being destroyed or the pool
     * counters from being updated.
     * </p>
     *
     * @param p A {@link PooledObject} wrapping the instance about to be
     *          destroyed.
     * @param reason The reason the object is being destroyed, never {@code null}.
     */
    void onDestroyObject(PooledObject<T> p, DestroyReason reason);
}
