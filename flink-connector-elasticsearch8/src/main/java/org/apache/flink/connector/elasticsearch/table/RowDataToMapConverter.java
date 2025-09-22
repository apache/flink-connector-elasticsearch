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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tool class used to convert from {@link RowData} to {@link Map}. * */
@Internal
public class RowDataToMapConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataType physicalDataType;

    public RowDataToMapConverter(DataType physicalDataType) {
        this.physicalDataType = physicalDataType;
    }

    public Map<String, Object> toMap(RowData rowData) {
        List<DataTypes.Field> fields = DataType.getFields(physicalDataType);

        Map<String, Object> map = new HashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            DataTypes.Field field = fields.get(i);
            RowData.FieldGetter fieldGetter =
                    RowData.createFieldGetter(field.getDataType().getLogicalType(), i);

            String key = field.getName();
            Object value =
                    DataStructureConverters.getConverter(field.getDataType())
                            .toExternalOrNull(fieldGetter.getFieldOrNull(rowData));

            map.put(key, value);
        }
        return map;
    }
}
