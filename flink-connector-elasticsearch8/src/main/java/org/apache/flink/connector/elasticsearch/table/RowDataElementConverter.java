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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;

import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of an {@link ElementConverter} for the ElasticSearch Table sink. The element
 * converter maps the Flink internal type of {@link RowData} to a {@link BulkOperationVariant} to be
 * used by Elasticsearch Java API
 */
@Internal
public class RowDataElementConverter implements ElementConverter<RowData, BulkOperationVariant> {
    private final IndexGenerator indexGenerator;
    private final Function<RowData, String> keyExtractor;
    private final RowDataToMapConverter rowDataToMapConverter;

    public RowDataElementConverter(
            DataType physicalDataType,
            IndexGenerator indexGenerator,
            Function<RowData, String> keyExtractor) {
        this.rowDataToMapConverter = new RowDataToMapConverter(physicalDataType);
        this.indexGenerator = indexGenerator;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void open(WriterInitContext context) {
        indexGenerator.open();
    }

    @Override
    public BulkOperationVariant apply(RowData rowData, SinkWriter.Context context) {
        Map<String, Object> dataMap = rowDataToMapConverter.toMap(rowData);

        BulkOperationVariant operation;

        switch (rowData.getRowKind()) {
            case INSERT:
                operation =
                        new IndexOperation.Builder<>()
                                .index(indexGenerator.generate(rowData))
                                .id(keyExtractor.apply(rowData))
                                .document(dataMap)
                                .build();
                break;
            case UPDATE_AFTER:
                operation =
                        new UpdateOperation.Builder<>()
                                .index(indexGenerator.generate(rowData))
                                .id(keyExtractor.apply(rowData))
                                .action(a -> a.doc(dataMap).docAsUpsert(true))
                                .build();
                break;
            case UPDATE_BEFORE:
            case DELETE:
                operation =
                        new DeleteOperation.Builder()
                                .index(indexGenerator.generate(rowData))
                                .id(keyExtractor.apply(rowData))
                                .build();
                break;
            default:
                throw new TableException("Unsupported message kind: " + rowData.getRowKind());
        }

        return operation;
    }
}
