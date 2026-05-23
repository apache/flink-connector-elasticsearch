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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/** OperationSerializer is responsible for serialization and deserialization of an Operation. */
public class OperationSerializer {
    private final Kryo kryo = new Kryo();

    public OperationSerializer() {
        kryo.setRegistrationRequired(false);
        ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
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
}
