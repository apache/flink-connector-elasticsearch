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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.LOWER;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.UPPER;

/** Utils for source to filter partition or row. */
public class FilterUtils {

    public static boolean shouldPushDown(ResolvedExpression expr, Set<String> filterableFields) {
        if (expr instanceof CallExpression && expr.getChildren().size() == 2) {
            return shouldPushDownUnaryExpression(
                            expr.getResolvedChildren().get(0), filterableFields)
                    && shouldPushDownUnaryExpression(
                            expr.getResolvedChildren().get(1), filterableFields);
        }
        return false;
    }

    private static boolean shouldPushDownUnaryExpression(
            ResolvedExpression expr, Set<String> filterableFields) {
        // validate that type is comparable
        if (!isComparable(expr.getOutputDataType().getConversionClass())) {
            return false;
        }
        if (expr instanceof FieldReferenceExpression) {
            if (filterableFields.contains(((FieldReferenceExpression) expr).getName())) {
                return true;
            }
        }

        if (expr instanceof ValueLiteralExpression) {
            return true;
        }

        if (expr instanceof CallExpression && expr.getChildren().size() == 1) {
            if (((CallExpression) expr).getFunctionDefinition().equals(UPPER)
                    || ((CallExpression) expr).getFunctionDefinition().equals(LOWER)) {
                return shouldPushDownUnaryExpression(
                        expr.getResolvedChildren().get(0), filterableFields);
            }
        }
        // other resolved expressions return false
        return false;
    }

    private static boolean isComparable(Class<?> clazz) {
        return Comparable.class.isAssignableFrom(clazz);
    }
}
