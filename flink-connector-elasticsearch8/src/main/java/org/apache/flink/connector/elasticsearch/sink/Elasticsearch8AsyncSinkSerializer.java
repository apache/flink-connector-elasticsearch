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

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** * Serializer for RetryableOperation.
 * Persists both the Operation data AND the retry counter.
 */
public class Elasticsearch8AsyncSinkSerializer extends AsyncSinkWriterStateSerializer<RetryableOperation> {

    @Override
    protected void serializeRequestToStream(RetryableOperation request, DataOutputStream out) throws IOException {
        out.writeInt(request.getAttemptCount());
        new OperationSerializer().serialize(request.getOperation(), out);
    }

    @Override
    protected RetryableOperation deserializeRequestFromStream(long requestSize, DataInputStream in) throws IOException {
        int attempts = in.readInt();
        Operation op = new OperationSerializer().deserialize(requestSize - 4, in);  // Read the Operation (Size is roughly requestSize - 4 bytes for the int)
        RetryableOperation retryableOp = new RetryableOperation(op);
        for (int i = 0; i < attempts; i++) {
            retryableOp.incrementAttempt();  // Fast-forward the counter to restored state
        }
        return retryableOp;
    }

    @Override
    public int getVersion() {
        return 1;
    }
}
