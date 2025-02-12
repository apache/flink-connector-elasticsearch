/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.flink.connector.elasticsearch.utils.RestClientUtils;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/** Implementation of {@link ElasticsearchApiCallBridge} for Elasticsearch 6 and later versions. */
@Internal
public class Elasticsearch6ApiCallBridge
        implements ElasticsearchApiCallBridge<RestHighLevelClient> {

    private static final long serialVersionUID = -5222683870097809633L;

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6ApiCallBridge.class);

    @Override
    public RestHighLevelClient createClient(
            NetworkClientConfig networkClientConfig, List<HttpHost> hosts) {
        return new RestHighLevelClient(
                RestClientUtils.configureRestClientBuilder(
                        RestClient.builder(hosts.toArray(new HttpHost[0])), networkClientConfig));
    }

    @Override
    public Tuple2<String, String[]> search(RestHighLevelClient client, SearchRequest searchRequest)
            throws IOException {
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        return new Tuple2<>(
                searchResponse.getScrollId(),
                Stream.of(searchHits).map(SearchHit::getSourceAsString).toArray(String[]::new));
    }

    @Override
    public void close(RestHighLevelClient client) throws IOException {
        client.close();
    }
}
