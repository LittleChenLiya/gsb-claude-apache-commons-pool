/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at

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
 * Enumerates the reasons a pooled object is destroyed when it is
 * {@link ObjectPool#returnObject(Object) returned} to an {@link ObjectPool} or
 * when the pool is closed.
 * <p>
 * Reasons are reported to a configured {@link ObjectDestroyListener} once per
 * destroyed object, immediately before
 * {@link PooledObjectFactory#destroyObject(PooledObject, DestroyMode)} is
 * invoked. Destruction paths not covered by this enumeration (for example
 * eviction, invalidation, activation failure or {@link ObjectPool#clear()})
 * are not reported.
 * </p>
 *
 * @see ObjectDestroyListener
 * @since 3.0.0
 */
public enum DestroyReason {

    /**
     * The object failed {@link PooledObjectFactory#validateObject(PooledObject)
     * validation} while being returned to the pool.
     */
    RETURN_VALIDATION_FAILED,

    /**
     * The object was returned to the pool but the configured
     * {@code maxIdle} limit had already been reached, so it was destroyed
     * instead of being idled.
     */
    MAX_IDLE_EXCEEDED,

    /**
     * The pool was closed. Idle objects are destroyed when the pool closes, and
     * objects returned to an already closed pool are also destroyed.
     */
    POOL_CLOSED
}
