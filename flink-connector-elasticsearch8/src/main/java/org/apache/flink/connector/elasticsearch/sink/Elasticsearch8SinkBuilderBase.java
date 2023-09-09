package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.InstantiationUtil;

import org.apache.http.HttpHost;

import javax.net.ssl.SSLContext;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base builder to construct a {@link Elasticsearch8Sink}.
 *
 * @param <IN> type of the records converted to Elasticsearch actions
 */
@PublicEvolving
public abstract class Elasticsearch8SinkBuilderBase
        <IN, B extends Elasticsearch8SinkBuilderBase<IN, B>> {

    private int bulkFlushMaxActions = 1000;
    private int bulkFlushMaxMb = -1;
    private long bulkFlushInterval = -1;
    private FlushBackoffType bulkFlushBackoffType = FlushBackoffType.NONE;
    private int bulkFlushBackoffRetries = -1;
    private long bulkFlushBackOffDelay = -1;
    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
    private List<HttpHost> hosts;
    private SSLContext sslContext;
    private Elasticsearch8Emitter<? super IN> emitter;
    private String username;
    private String password;
    private String connectionPathPrefix;
    private Integer connectionTimeout;
    private Integer connectionRequestTimeout;
    private Integer socketTimeout;

    protected Elasticsearch8SinkBuilderBase() {}

    @SuppressWarnings("unchecked")
    protected <S extends Elasticsearch8SinkBuilderBase<?, ?>> S self() {
        return (S) this;
    }

    public <T extends IN> Elasticsearch8SinkBuilderBase<T, ?> setEmitter(
            Elasticsearch8Emitter<? super T> emitter) {
        checkNotNull(emitter);
        checkState(
                InstantiationUtil.isSerializable(emitter),
                "The elasticsearch emitter must be serializable.");

        final Elasticsearch8SinkBuilderBase<T, ?> self = self();
        self.emitter = emitter;
        return self;
    }

    public B setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkState(hosts.length > 0, "Hosts cannot be empty.");
        this.hosts = Arrays.asList(hosts);
        return self();
    }

    public B setSSLContext(SSLContext sslContext) {
        this.sslContext = sslContext;
        return self();
    }

    public B setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        checkState(
                deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
                "Elasticsearch sink does not support the EXACTLY_ONCE guarantee.");
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
        return self();
    }

    public B setBulkFlushMaxActions(int numMaxActions) {
        checkState(
                numMaxActions == -1 || numMaxActions > 0,
                "Max number of buffered actions must be larger than 0.");
        this.bulkFlushMaxActions = numMaxActions;
        return self();
    }

    public B setBulkFlushMaxSizeMb(int maxSizeMb) {
        checkState(
                maxSizeMb == -1 || maxSizeMb > 0,
                "Max size of buffered actions must be larger than 0.");
        this.bulkFlushMaxMb = maxSizeMb;
        return self();
    }

    public B setBulkFlushInterval(long intervalMillis) {
        checkState(
                intervalMillis == -1 || intervalMillis >= 0,
                "Interval (in milliseconds) between each flush must be larger than "
                        + "or equal to 0.");
        this.bulkFlushInterval = intervalMillis;
        return self();
    }

    public B setBulkFlushBackoffStrategy(
            FlushBackoffType flushBackoffType, int maxRetries, long delayMillis) {
        this.bulkFlushBackoffType = checkNotNull(flushBackoffType);
        checkState(
                flushBackoffType != FlushBackoffType.NONE,
                "FlushBackoffType#NONE does not require a configuration it is the default, retries and delay are ignored.");
        checkState(maxRetries > 0, "Max number of backoff attempts must be larger than 0.");
        this.bulkFlushBackoffRetries = maxRetries;
        checkState(
                delayMillis >= 0,
                "Delay (in milliseconds) between each backoff attempt must be larger "
                        + "than or equal to 0.");
        this.bulkFlushBackOffDelay = delayMillis;
        return self();
    }

    public B setConnectionUsername(String username) {
        checkNotNull(username);
        this.username = username;
        return self();
    }

    public B setConnectionPassword(String password) {
        checkNotNull(password);
        this.password = password;
        return self();
    }

    public B setConnectionPathPrefix(String prefix) {
        checkNotNull(prefix);
        this.connectionPathPrefix = prefix;
        return self();
    }

    public B setConnectionRequestTimeout(int timeout) {
        checkState(timeout >= 0, "Connection request timeout must be larger than or equal to 0.");
        this.connectionRequestTimeout = timeout;
        return self();
    }

    public B setConnectionTimeout(int timeout) {
        checkState(timeout >= 0, "Connection timeout must be larger than or equal to 0.");
        this.connectionTimeout = timeout;
        return self();
    }

    public B setSocketTimeout(int timeout) {
        checkState(timeout >= 0, "Socket timeout must be larger than or equal to 0.");
        this.socketTimeout = timeout;
        return self();
    }

    protected abstract BulkProcessorFactory<IN> getBulkProcessorBuilderFactory();

    public Elasticsearch8Sink<IN> build() {
        checkNotNull(emitter);
        checkNotNull(hosts);

        NetworkClientConfig networkClientConfig = buildNetworkClientConfig();
        BulkProcessorConfig bulkProcessorConfig = buildBulkProcessorConfig();

        BulkProcessorFactory<IN> bulkProcessorFactory = getBulkProcessorBuilderFactory();
        ClosureCleaner.clean(
                bulkProcessorFactory, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        return new Elasticsearch8Sink<>(
                hosts,
                sslContext,
                emitter,
                deliveryGuarantee,
                bulkProcessorFactory,
                bulkProcessorConfig,
                networkClientConfig);
    }

    private NetworkClientConfig buildNetworkClientConfig() {
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");

        return new NetworkClientConfig(
                username,
                password,
                connectionPathPrefix,
                connectionRequestTimeout,
                connectionTimeout,
                socketTimeout);
    }

    private BulkProcessorConfig buildBulkProcessorConfig() {
        return new BulkProcessorConfig(
                bulkFlushMaxActions,
                bulkFlushMaxMb,
                bulkFlushInterval,
                bulkFlushBackoffType,
                bulkFlushBackoffRetries,
                bulkFlushBackOffDelay);
    }

}
