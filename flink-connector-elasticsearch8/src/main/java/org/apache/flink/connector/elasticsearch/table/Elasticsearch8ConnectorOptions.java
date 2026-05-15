/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.time.Duration;
import java.util.List;

/**
 * Base options for the Elasticsearch connector. Needs to be public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
@PublicEvolving
public class Elasticsearch8ConnectorOptions {

    Elasticsearch8ConnectorOptions() {}

    public static final ConfigOption<List<String>> HOSTS_OPTION =
            ConfigOptions.key("hosts")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Elasticsearch hosts to connect to.");

    public static final ConfigOption<String> INDEX_OPTION =
            ConfigOptions.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Elasticsearch index for every record.");

    public static final ConfigOption<String> PASSWORD_OPTION =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password used to connect to Elasticsearch instance.");

    public static final ConfigOption<String> USERNAME_OPTION =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username used to connect to Elasticsearch instance.");

    public static final ConfigOption<String> KEY_DELIMITER_OPTION =
            ConfigOptions.key("document-id.key-delimiter")
                    .stringType()
                    .defaultValue("_")
                    .withDescription(
                            "Delimiter for composite keys e.g., \"$\" would result in IDs \"KEY1$KEY2$KEY3\".");

    public static final ConfigOption<Integer> BULK_FLUSH_MAX_ACTIONS_OPTION =
            ConfigOptions.key("sink.bulk-flush.max-actions")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum number of actions for each bulk request.");

    public static final ConfigOption<Integer> BULK_FLUSH_MAX_BUFFERED_ACTIONS_OPTION =
            ConfigOptions.key("sink.bulk-flush.max-buffered-actions")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum buffer length for actions");

    public static final ConfigOption<Integer> BULK_FLUSH_MAX_IN_FLIGHT_ACTIONS_OPTION =
            ConfigOptions.key("sink.bulk-flush.max-in-flight-actions")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "Threshold for uncompleted actions before blocking new write actions.");

    public static final ConfigOption<MemorySize> BULK_FLUSH_MAX_SIZE_OPTION =
            ConfigOptions.key("sink.bulk-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("2mb"))
                    .withDescription("Maximum size of buffered actions per bulk request");

    public static final ConfigOption<Duration> BULK_FLUSH_INTERVAL_OPTION =
            ConfigOptions.key("sink.bulk-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Bulk flush interval");

    public static final ConfigOption<String> CONNECTION_PATH_PREFIX_OPTION =
            ConfigOptions.key("connection.path-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prefix string to be added to every REST communication.");

    public static final ConfigOption<Duration> CONNECTION_REQUEST_TIMEOUT =
            ConfigOptions.key("connection.request-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The timeout for requesting a connection from the connection manager.");

    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
            ConfigOptions.key("connection.timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The timeout for establishing a connection.");

    public static final ConfigOption<Duration> SOCKET_TIMEOUT =
            ConfigOptions.key("socket.timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The socket timeout (SO_TIMEOUT) for waiting for data or, put differently,"
                                    + "a maximum period inactivity between two consecutive data packets.");

    public static final ConfigOption<String> SSL_CERTIFICATE_FINGERPRINT =
            ConfigOptions.key("ssl.certificate-fingerprint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The HTTP CA certificate SHA-256 fingerprint used to verify the HTTPS connection.");

    public static final ConfigOption<String> FORMAT_OPTION =
            ConfigOptions.key("format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "The format must produce a valid JSON document. "
                                    + "Please refer to the documentation on formats for more details.");

    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE_OPTION =
            ConfigOptions.key("sink.delivery-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
                    .withDescription("Optional delivery guarantee when committing.");

    // --- Lookup / vector search options ----------------------------------------------------

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withFallbackKeys("lookup.max-retries")
                    .withDescription(
                            "The maximum allowed retries if a lookup/search operation fails.");

    public static final ConfigOption<Integer> NUM_CANDIDATES =
            ConfigOptions.key("vector-search.num-candidates")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The number of candidate neighbors considered for each shard during the vector search.");
}
