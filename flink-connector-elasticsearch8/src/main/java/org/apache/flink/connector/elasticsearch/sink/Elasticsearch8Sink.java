package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;

import org.apache.http.HttpHost;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink Sink to insert or update data in an Elasticsearch index. The sink supports the following
 * delivery guarantees.
 *
 * <ul>
 *   <li>{@link DeliveryGuarantee#NONE} does not provide any guarantees: actions are flushed to
 *       Elasticsearch only depending on the configurations of the bulk processor. In case of a
 *       failure, it might happen that actions are lost if the bulk processor still has buffered
 *       actions.
 *   <li>{@link DeliveryGuarantee#AT_LEAST_ONCE} on a checkpoint the sink will wait until all
 *       buffered actions are flushed to and acknowledged by Elasticsearch. No actions will be lost
 *       but actions might be sent to Elasticsearch multiple times when Flink restarts. These
 *       additional requests may cause inconsistent data in ElasticSearch right after the restart,
 *       but eventually everything will be consistent again.
 * </ul>
 *
 * @param <IN> type of the records converted to Elasticsearch actions
 * @see Elasticsearch8SinkBuilderBase on how to construct a ElasticsearchSink
 */
@PublicEvolving
public class Elasticsearch8Sink<IN> implements Sink<IN> {

    private final List<HttpHost> hosts;
    private final SSLContext sslContext;
    private final Elasticsearch8Emitter<? super IN> emitter;
    private final BulkProcessorConfig buildBulkProcessorConfig;
    private final BulkProcessorFactory<IN> bulkProcessorFactory;
    private final NetworkClientConfig networkClientConfig;
    private final DeliveryGuarantee deliveryGuarantee;

    Elasticsearch8Sink(
            List<HttpHost> hosts,
            SSLContext sslContext,
            Elasticsearch8Emitter<? super IN> emitter,
            DeliveryGuarantee deliveryGuarantee,
            BulkProcessorFactory<IN> bulkProcessorFactory,
            BulkProcessorConfig buildBulkProcessorConfig,
            NetworkClientConfig networkClientConfig) {
        this.hosts = checkNotNull(hosts);
        this.sslContext = sslContext;
        this.bulkProcessorFactory = checkNotNull(bulkProcessorFactory);
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");
        this.emitter = checkNotNull(emitter);
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
        this.buildBulkProcessorConfig = checkNotNull(buildBulkProcessorConfig);
        this.networkClientConfig = checkNotNull(networkClientConfig);
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        return new Elasticsearch8Writer<IN>(
                hosts,
                sslContext,
                emitter,
                deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE,
                buildBulkProcessorConfig,
                bulkProcessorFactory,
                networkClientConfig,
                context.metricGroup(),
                context.getMailboxExecutor());
    }

    @VisibleForTesting
    DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }
}
