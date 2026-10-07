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
 * Enumerates the reasons why a pool destroys a pooled object on the return or
 * close path. Values are reported to the optional
 * {@link DestroyObjectCallback} configured on the pool.
 *
 * @since 3.0.0
 */
public enum DestroyReason {

    /** The object failed validation when it was returned to the pool. */
    RETURN_VALIDATION_FAILED,

    /** The object was returned while the pool already held the configured maximum number of idle objects. */
    MAX_IDLE_EXCEEDED,

    /** The object was destroyed because the pool was closed. */
    POOL_CLOSED
}
