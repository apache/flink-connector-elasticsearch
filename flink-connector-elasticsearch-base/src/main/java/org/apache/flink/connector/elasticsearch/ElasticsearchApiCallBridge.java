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

package org.apache.flink.connector.elasticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * An {@link ElasticsearchApiCallBridge} is used to bridge incompatible Elasticsearch Java API calls
 * across different versions. This includes calls to create Elasticsearch clients, handle failed
 * item responses, etc. Any incompatible Elasticsearch Java APIs should be bridged using this
 * interface.
 *
 * <p>Implementations are allowed to be stateful. For example, for Elasticsearch 1.x, since
 * connecting via an embedded node is allowed, the call bridge will hold reference to the created
 * embedded node. Each instance of the sink will hold exactly one instance of the call bridge, and
 * state cleanup is performed when the sink is closed.
 *
 * @param <C> The Elasticsearch client, that implements {@link AutoCloseable}.
 */
@Internal
public interface ElasticsearchApiCallBridge<C extends AutoCloseable> extends Serializable {

    /**
     * Creates an Elasticsearch client implementing {@link AutoCloseable}.
     *
     * @return The created client.
     */
    C createClient(NetworkClientConfig networkClientConfig, List<HttpHost> hosts);

    /**
     * Executes a search using the Search API.
     *
     * @param client the Elasticsearch client.
     * @param searchRequest A request to execute search against one or more indices (or all).
     */
    Tuple2<String, String[]> search(C client, SearchRequest searchRequest) throws IOException;

    /**
     * Closes this client and releases any system resources associated with it.
     *
     * @param client the Elasticsearch client.
     */
    void close(C client) throws IOException;
}
