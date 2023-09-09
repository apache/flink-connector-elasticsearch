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

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.connector.elasticsearch.RequestTest;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.GetResponse;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

abstract class TestClientBase {

    private static final String DATA_FIELD_NAME = "data";
    final ElasticsearchClient client;

    TestClientBase(ElasticsearchClient client) {
        this.client = client;
    }

    abstract GetResponse getResponse(String index, int id)
            throws ElasticsearchException, IOException;

    void assertThatIdsAreNotWritten(String index, int... ids)
            throws ElasticsearchException, IOException {
        for (final int id : ids) {
            try {
                final GetResponse response = getResponse(index, id);
                assertThat(response.found())
                        .as(String.format("Index %s Id %s is unexpectedly present.", index, id))
                        .isFalse();
            } catch (ElasticsearchException e) {
                assertThat(e.status()).isEqualTo(404);
            } catch (IOException ioe) {
                throw ioe;
            }
        }
    }

    void assertThatIdsAreWritten(String index, int... ids)
            throws IOException, InterruptedException {
        for (final int id : ids) {
            GetResponse response;
            do {
                response = getResponse(index, id);
                Thread.sleep(1);
            } while (!response.found());
            assertThat(response.source()).isEqualTo(buildMessage(id));
        }
    }

    String getDataFieldName() {
        return DATA_FIELD_NAME;
    }

    static RequestTest.Product buildMessage(int id) {
        return buildMessage(id, "test-" + id);
    }

    static RequestTest.Product buildMessage(int id, String name) {
        return new RequestTest.Product(id, name);
    }
}
