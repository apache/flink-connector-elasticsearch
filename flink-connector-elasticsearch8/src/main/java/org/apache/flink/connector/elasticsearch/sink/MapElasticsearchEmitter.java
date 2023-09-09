package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.elasticsearch.bulk.RequestIndexer;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Function;

/** A simple Elasticsearch8Emitter which is currently used in PyFlink ES connector. */
public class MapElasticsearchEmitter implements Elasticsearch8Emitter<Map<String, Object>> {

    private static final long serialVersionUID = 1L;

    private final String index;
    private @Nullable final String idFieldName;
    private final boolean isDynamicIndex;
    private transient Function<Map<String, Object>, String> indexProvider;

    public MapElasticsearchEmitter(
            String index,
            @Nullable String idFieldName,
            boolean isDynamicIndex,
            Function<Map<String, Object>, String> indexProvider) {
        this.index = index;
        this.idFieldName = idFieldName;
        this.isDynamicIndex = isDynamicIndex;
        this.indexProvider = indexProvider;
    }

    @Override
    public void open() throws Exception {
        if (isDynamicIndex) {
            indexProvider = doc -> doc.get(index).toString();
        } else {
            indexProvider = doc -> index;
        }
    }

    @Override
    public void close() throws Exception {
        Elasticsearch8Emitter.super.close();
    }

    @Override
    public void emit(Map<String, Object> doc, SinkWriter.Context context, RequestIndexer indexer) {
        if (idFieldName != null) {
            UpdateOperation<Object, Object> updateOperation =
                    UpdateOperation.of(
                            b -> b.index(String.valueOf(index)).id(indexProvider.apply(doc))
                                    .action(_3 -> _3.docAsUpsert(true).doc(doc)));
            BulkOperation bulkOperation = BulkOperation.of(f -> f.update(updateOperation));
            indexer.add(bulkOperation);
        } else {
            {
                CreateOperation<Object> createOperation =
                        CreateOperation.of(
                                b -> b.index(String.valueOf(index)).id(indexProvider.apply(doc))
                                        .document(doc));
                BulkOperation bulkOperation = BulkOperation.of(f -> f.create(createOperation));
                indexer.add(bulkOperation);
            }
        }
    }
}
