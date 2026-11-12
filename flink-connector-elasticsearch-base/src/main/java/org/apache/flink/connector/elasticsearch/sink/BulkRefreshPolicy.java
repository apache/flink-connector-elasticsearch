/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Refresh policy of the bulk requests sent to Elasticsearch, i.e. the {@code refresh} parameter of
 * the Bulk API.
 *
 * <p>Both {@link #IMMEDIATE} and {@link #WAIT_UNTIL} reduce the sink throughput. They are only
 * recommended for low-throughput scenarios that require the written data to be searchable soon.
 */
@PublicEvolving
public enum BulkRefreshPolicy {
    /**
     * Do not refresh ({@code refresh=false}). The written data becomes searchable after the next
     * periodic refresh of the index. This is the default.
     */
    NONE,

    /**
     * Refresh the affected shards right after the bulk request ({@code refresh=true}), so the
     * written data is searchable once the request returns.
     */
    IMMEDIATE,

    /**
     * Do not force a refresh, but wait until the next periodic refresh makes the written data
     * searchable before the request returns ({@code refresh=wait_for}). Each bulk request may be
     * blocked for up to {@code index.refresh_interval}.
     */
    WAIT_UNTIL
}
