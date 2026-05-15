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

package org.apache.flink.connector.elasticsearch.sink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/** OperationSerializer is responsible for serialization and deserialization of an Operation. */
public class OperationSerializer {
    private final Kryo kryo = new Kryo();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public OperationSerializer() {
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.addDefaultSerializer(JsonNode.class, new JsonNodeSerializer());
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    }

    public void serialize(Operation request, DataOutputStream out) {
        try (Output output = new Output(out)) {
            kryo.writeObject(output, request);
            output.flush();
        }
    }

    public Operation deserialize(long requestSize, DataInputStream in) {
        try (Input input = new Input(in, (int) requestSize)) {
            return kryo.readObject(input, Operation.class);
        }
    }

    public int size(Operation operation) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (Output output = new Output(byteArrayOutputStream)) {
            kryo.writeObject(output, operation);
            output.flush();

            return (int) output.total();
        }
    }

    private static class JsonNodeSerializer extends Serializer<JsonNode> {
        @Override
        public void write(Kryo kryo, Output output, JsonNode object) {
            try {
                byte[] bytes = MAPPER.writeValueAsBytes(object);
                output.writeInt(bytes.length, true);
                output.writeBytes(bytes);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize JsonNode", e);
            }
        }

        @Override
        public JsonNode read(Kryo kryo, Input input, Class<JsonNode> type) {
            try {
                int length = input.readInt(true);
                byte[] bytes = input.readBytes(length);
                return MAPPER.readTree(bytes);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize JsonNode", e);
            }
        }
    }
}
