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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.connector.elasticsearch.test.DockerImageVersions;

/** End-to-end test for Elasticsearch6 lookup. */
public class Elasticsearch6LookupE2ECase extends ElasticsearchLookupE2ECase {
    @Override
    String getElasticsearchContainerName() {
        return DockerImageVersions.ELASTICSEARCH_6;
    }

    @Override
    String getEsOptions() {
        return " 'connector' = 'elasticsearch-6',"
                + " 'hosts' = '"
                + "http://"
                + elasticsearchContainer.getHttpHostAddress()
                + "',"
                + "'document-type' = '_doc',"
                + "'index' = '"
                + ES_INDEX
                + "',"
                + "'lookup.cache' = 'partial',"
                + "'lookup.partial-cache.max-rows' = '100'";
    }
}
