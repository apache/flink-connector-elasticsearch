/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.util;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/** Utils for handling exceptions. */
public final class ExceptionUtils {
    public static <T extends Throwable> T firstOrSuppressed(T newException, @Nullable T previous) {
        Preconditions.checkNotNull(newException, "newException");
        if (previous != null && previous != newException) {
            previous.addSuppressed(newException);
            return previous;
        } else {
            return newException;
        }
    }
}
