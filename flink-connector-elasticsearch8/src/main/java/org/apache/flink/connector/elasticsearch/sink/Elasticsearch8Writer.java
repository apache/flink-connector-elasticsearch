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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.elasticsearch.bulk.BulkProcessor;
import org.apache.flink.connector.elasticsearch.bulk.RequestIndexer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;

class Elasticsearch8Writer<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8Writer.class);

    private final SSLContext sslContext;
    private final Elasticsearch8Emitter<? super IN> emitter;
    private final MailboxExecutor mailboxExecutor;
    private final boolean flushOnCheckpoint;
    private final BulkProcessor<IN> bulkProcessor;
    private final ElasticsearchClient client;
    private final RequestIndexer requestIndexer;
    private final Counter numBytesOutCounter;

    private long pendingActions = 0;
    private boolean checkpointInProgress = false;
    private volatile long lastSendTime = 0;
    private volatile long ackTime = Long.MAX_VALUE;
    private volatile boolean closed = false;

    public Elasticsearch8Writer(
            List<HttpHost> hosts,
            SSLContext sslContext,
            Elasticsearch8Emitter<? super IN> emitter,
            boolean flushOnCheckpoint,
            BulkProcessorConfig bulkProcessorConfig,
            BulkProcessorFactory<IN> bulkProcessorFactory,
            NetworkClientConfig networkClientConfig,
            SinkWriterMetricGroup metricGroup,
            MailboxExecutor mailboxExecutor)
            throws IOException {
        this.sslContext = sslContext == null ? getSSLContext() : sslContext;
        this.emitter = checkNotNull(emitter);
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.client = createClient(hosts, networkClientConfig);
        this.bulkProcessor = createBulkProcessor(bulkProcessorFactory, bulkProcessorConfig);
        this.requestIndexer =
                new Elasticsearch8Writer<IN>.DefaultRequestIndexer(
                        metricGroup.getNumRecordsSendCounter());
        checkNotNull(metricGroup);
        metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);
        this.numBytesOutCounter = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
        try {
            emitter.open();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the ElasticsearchEmitter", e);
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        // do not allow new bulk writes until all actions are flushed
        while (checkpointInProgress) {
            mailboxExecutor.yield();
        }
        emitter.emit(element, context, requestIndexer);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkpointInProgress = true;
        while (pendingActions != 0 && (flushOnCheckpoint || endOfInput)) {
            bulkProcessor.flush();
            LOG.info("Waiting for the response of {} pending actions.", pendingActions);
            mailboxExecutor.yield();
        }
        checkpointInProgress = false;
    }

    @VisibleForTesting
    void blockingFlushAllActions() throws InterruptedException {
        while (pendingActions != 0) {
            bulkProcessor.flush();
            LOG.info("Waiting for the response of {} pending actions.", pendingActions);
            mailboxExecutor.yield();
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        emitter.close();
        bulkProcessor.close();
        client.shutdown();
    }

    private static ElasticsearchClient createClient(
            List<HttpHost> httpHosts, NetworkClientConfig networkClientConfig) throws IOException {
        String username = networkClientConfig.getUsername();
        String password = networkClientConfig.getPassword();
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (StringUtils.isNoneEmpty(username) && StringUtils.isNoneEmpty(password)) {
            credentialsProvider.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        }

        RestClientBuilder builder =
                RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
        RestClientBuilder restClientBuilder =
                configureRestClientBuilder(builder, networkClientConfig);
        RestClient httpClient = restClientBuilder.build();
        ElasticsearchTransport transport =
                new RestClientTransport(httpClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    private static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, NetworkClientConfig networkClientConfig) throws IOException {
        if (networkClientConfig.getConnectionPathPrefix() != null) {
            builder.setPathPrefix(networkClientConfig.getConnectionPathPrefix());
        }
        if (networkClientConfig.getPassword() != null
                && networkClientConfig.getUsername() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(
                            networkClientConfig.getUsername(), networkClientConfig.getPassword()));
            SSLContext sslContext = getSSLContext();
            builder.setHttpClientConfigCallback(
                    b ->
                            b.setDefaultCredentialsProvider(credentialsProvider)
                                    .setSSLContext(sslContext));
        }
        if (networkClientConfig.getConnectionRequestTimeout() != null
                || networkClientConfig.getConnectionTimeout() != null
                || networkClientConfig.getSocketTimeout() != null) {
            builder.setRequestConfigCallback(
                    requestConfigBuilder -> {
                        if (networkClientConfig.getConnectionRequestTimeout() != null) {
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    networkClientConfig.getConnectionRequestTimeout());
                        }
                        if (networkClientConfig.getConnectionTimeout() != null) {
                            requestConfigBuilder.setConnectTimeout(
                                    networkClientConfig.getConnectionTimeout());
                        }
                        if (networkClientConfig.getSocketTimeout() != null) {
                            requestConfigBuilder.setSocketTimeout(
                                    networkClientConfig.getSocketTimeout());
                        }
                        return requestConfigBuilder;
                    });
        }
        return builder;
    }

    private static SSLContext getSSLContext() throws IOException {
        TrustManager[] trustAllCerts =
                new TrustManager[] {
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(
                                java.security.cert.X509Certificate[] chain, String authType) {}

                        @Override
                        public void checkServerTrusted(
                                java.security.cert.X509Certificate[] chain, String authType) {}

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[] {};
                        }
                    }
                };

        SSLContext sslContext = null;
        try {
            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
        } catch (Exception e) {
            throw new IOException(e);
        }

        return sslContext;
    }

    private BulkProcessor<IN> createBulkProcessor(
            BulkProcessorFactory<IN> bulkProcessorFactory,
            BulkProcessorConfig bulkProcessorConfig) {

        BulkProcessor.Builder<IN> builder =
                bulkProcessorFactory.apply(
                        client, bulkProcessorConfig, new Elasticsearch8Writer<IN>.BulkListener());

        return builder.build();
    }

    private class BulkListener implements BulkProcessor.BulkListener<IN> {
        @Override
        public void beforeBulk(long executionId, BulkRequest request, List list) {
            LOG.info("Sending bulk of {} actions to Elasticsearch.", request.operations());
            lastSendTime = System.currentTimeMillis();
            numBytesOutCounter.inc(request.operations().size());
        }

        @Override
        public void afterBulk(
                long executionId, BulkRequest request, List list, BulkResponse response) {
            ackTime = System.currentTimeMillis();
            enqueueActionInMailbox(
                    () -> extractFailures(request, response), "elasticsearchSuccessCallback");
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, List list, Throwable failure) {
            enqueueActionInMailbox(
                    () -> {
                        throw new FlinkRuntimeException("Complete bulk has failed.", failure);
                    },
                    "elasticsearchErrorCallback");
        }
    }

    private void enqueueActionInMailbox(
            ThrowingRunnable<? extends Exception> action, String actionName) {
        // If the writer is cancelled before the last bulk response (i.e. no flush on checkpoint
        // configured or shutdown without a final
        // checkpoint) the mailbox might already be shutdown, so we should not enqueue any
        // actions.
        if (isClosed()) {
            return;
        }
        mailboxExecutor.execute(action, actionName);
    }

    private void extractFailures(BulkRequest request, BulkResponse response) {
        if (!response.errors()) {
            LOG.debug(
                    "Current pendingActions: {}, dec: {}",
                    pendingActions,
                    request.operations().size());
            pendingActions -= request.operations().size();
            return;
        }

        Throwable chainedFailures = null;
        for (int i = 0; i < response.items().size(); i++) {
            BulkResponseItem itemResponse = response.items().get(i);
            int status = itemResponse.status();
            if (status == 0) {
                continue;
            }
            ErrorCause failure = itemResponse.error();
            if (failure == null) {
                continue;
            }
            final BulkOperation bulkOperation = request.operations().get(i);
            chainedFailures =
                    firstOrSuppressed(
                            wrapException(status, new Exception(failure.reason()), bulkOperation),
                            chainedFailures);
        }
        if (chainedFailures == null) {
            return;
        }
        LOG.error(chainedFailures.getMessage(), new FlinkRuntimeException(chainedFailures));
        throw new FlinkRuntimeException(chainedFailures);
    }

    private static Throwable wrapException(
            int restStatus, Throwable rootFailure, BulkOperation bulkOperation) {
        return new FlinkRuntimeException(
                String.format(
                        "Single action %s of bulk request failed with status %s.",
                        bulkOperation, restStatus),
                rootFailure);
    }

    private boolean isClosed() {
        if (closed) {
            LOG.warn("Writer was closed before all records were acknowledged by Elasticsearch.");
        }
        return closed;
    }

    private class DefaultRequestIndexer implements RequestIndexer {

        private final Counter numRecordsSendCounter;

        public DefaultRequestIndexer(Counter numRecordsSendCounter) {
            this.numRecordsSendCounter = checkNotNull(numRecordsSendCounter);
        }

        @Override
        public void add(BulkOperation... operations) {
            for (final BulkOperation operation : operations) {
                numRecordsSendCounter.inc();
                pendingActions++;
                bulkProcessor.add(operation);
            }
        }
    }
}
