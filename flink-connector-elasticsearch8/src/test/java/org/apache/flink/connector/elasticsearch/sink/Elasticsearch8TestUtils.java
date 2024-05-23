package org.apache.flink.connector.elasticsearch.sink;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Utility class for Elasticsearch8 tests. */
public class Elasticsearch8TestUtils {
    private Elasticsearch8TestUtils() {}

    public static final String ELASTICSEARCH_VERSION = "8.12.1";

    public static final DockerImageName ELASTICSEARCH_IMAGE =
            DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                    .withTag(ELASTICSEARCH_VERSION);

    /** DummyData is a POJO to helping during integration tests. */
    public static class DummyData {
        private final String id;

        private final String name;

        public DummyData(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    public static void assertIdsAreWritten(RestClient client, String index, String[] ids)
            throws IOException {
        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", index + "/_search/"));
        String responseEntity = EntityUtils.toString(response.getEntity());

        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

        for (String id : ids) {
            System.out.println(id);
            assertThat(responseEntity).contains(id);
        }
    }

    public static void assertIdsAreNotWritten(RestClient client, String index, String[] ids)
            throws IOException {
        client.performRequest(new Request("GET", "_refresh"));
        Response response = client.performRequest(new Request("GET", index + "/_search/"));
        String responseEntity = EntityUtils.toString(response.getEntity());

        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

        for (String id : ids) {
            assertThat(responseEntity).doesNotContain(id);
        }
    }
}
