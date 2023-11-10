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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.ProxyMetricGroup;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** Testing implementation for {@link SinkWriterMetricGroup}. */
public class TestingSinkWriterMetricGroup extends ProxyMetricGroup<MetricGroup>
        implements SinkWriterMetricGroup {

    private final Supplier<Counter> numRecordsOutErrorsCounterSupplier;

    private final Supplier<Counter> numRecordsSendErrorsCounterSupplier;

    private final Supplier<Counter> numRecordsSendCounterSupplier;

    private final Supplier<Counter> numBytesSendCounterSupplier;

    private final Consumer<Gauge<Long>> currentSendTimeGaugeConsumer;

    private final Supplier<OperatorIOMetricGroup> ioMetricGroupSupplier;

    public TestingSinkWriterMetricGroup(
            MetricGroup parentMetricGroup,
            Supplier<Counter> numRecordsOutErrorsCounterSupplier,
            Supplier<Counter> numRecordsSendErrorsCounterSupplier,
            Supplier<Counter> numRecordsSendCounterSupplier,
            Supplier<Counter> numBytesSendCounterSupplier,
            Consumer<Gauge<Long>> currentSendTimeGaugeConsumer,
            Supplier<OperatorIOMetricGroup> ioMetricGroupSupplier) {
        super(parentMetricGroup);
        this.numRecordsOutErrorsCounterSupplier = numRecordsOutErrorsCounterSupplier;
        this.numRecordsSendErrorsCounterSupplier = numRecordsSendErrorsCounterSupplier;
        this.numRecordsSendCounterSupplier = numRecordsSendCounterSupplier;
        this.numBytesSendCounterSupplier = numBytesSendCounterSupplier;
        this.currentSendTimeGaugeConsumer = currentSendTimeGaugeConsumer;
        this.ioMetricGroupSupplier = ioMetricGroupSupplier;
    }

    @Override
    public Counter getNumRecordsOutErrorsCounter() {
        return numRecordsOutErrorsCounterSupplier.get();
    }

    @Override
    public Counter getNumRecordsSendErrorsCounter() {
        return numRecordsSendErrorsCounterSupplier.get();
    }

    @Override
    public Counter getNumRecordsSendCounter() {
        return numRecordsSendCounterSupplier.get();
    }

    @Override
    public Counter getNumBytesSendCounter() {
        return numBytesSendCounterSupplier.get();
    }

    @Override
    public void setCurrentSendTimeGauge(Gauge<Long> gauge) {
        currentSendTimeGaugeConsumer.accept(gauge);
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return ioMetricGroupSupplier.get();
    }

    /** Builder for {@link TestingSinkWriterMetricGroup}. */
    public static class Builder {

        private MetricGroup parentMetricGroup = null;

        private Supplier<Counter> numRecordsOutErrorsCounterSupplier = () -> null;

        private Supplier<Counter> numRecordsSendErrorsCounterSupplier = () -> null;

        private Supplier<Counter> numRecordsSendCounterSupplier = () -> null;

        private Supplier<Counter> numBytesSendCounterSupplier = () -> null;

        private Consumer<Gauge<Long>> currentSendTimeGaugeConsumer = counter -> {};

        private Supplier<OperatorIOMetricGroup> ioMetricGroupSupplier = () -> null;

        public Builder setParentMetricGroup(MetricGroup parentMetricGroup) {
            this.parentMetricGroup = parentMetricGroup;
            return this;
        }

        public Builder setNumRecordsOutErrorsCounterSupplier(
                Supplier<Counter> numRecordsOutErrorsCounterSupplier) {
            this.numRecordsOutErrorsCounterSupplier = numRecordsOutErrorsCounterSupplier;
            return this;
        }

        public Builder setNumRecordsSendErrorsCounterSupplier(
                Supplier<Counter> numRecordsSendErrorsCounterSupplier) {
            this.numRecordsSendErrorsCounterSupplier = numRecordsSendErrorsCounterSupplier;
            return this;
        }

        public Builder setNumRecordsSendCounterSupplier(
                Supplier<Counter> numRecordsSendCounterSupplier) {
            this.numRecordsSendCounterSupplier = numRecordsSendCounterSupplier;
            return this;
        }

        public Builder setNumBytesSendCounterSupplier(
                Supplier<Counter> numBytesSendCounterSupplier) {
            this.numBytesSendCounterSupplier = numBytesSendCounterSupplier;
            return this;
        }

        public Builder setCurrentSendTimeGaugeConsumer(
                Consumer<Gauge<Long>> currentSendTimeGaugeConsumer) {
            this.currentSendTimeGaugeConsumer = currentSendTimeGaugeConsumer;
            return this;
        }

        public Builder setIoMetricGroupSupplier(
                Supplier<OperatorIOMetricGroup> ioMetricGroupSupplier) {
            this.ioMetricGroupSupplier = ioMetricGroupSupplier;
            return this;
        }

        public TestingSinkWriterMetricGroup build() {
            return new TestingSinkWriterMetricGroup(
                    parentMetricGroup,
                    numRecordsOutErrorsCounterSupplier,
                    numRecordsSendErrorsCounterSupplier,
                    numRecordsSendCounterSupplier,
                    numBytesSendCounterSupplier,
                    currentSendTimeGaugeConsumer,
                    ioMetricGroupSupplier);
        }
    }
}
