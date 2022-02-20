/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.zabetak.calcite.tutorial.rules;

import org.apache.calcite.rel.logical.LogicalFilter;

import com.github.zabetak.calcite.tutorial.operators.LuceneFilter;

/**
 * Rule to convert a {@link LogicalFilter} to a {@link LuceneFilter} if possible.
 *
 * The filter can be pushed in Lucene if it is of the following form.
 *
 * <pre>{@code
 * =($0, 154)
 * }</pre>
 *
 * A single equality operator with input reference on the left side and an integer literal on the
 * right side. The input reference should be resolvable to an actual column of the table.
 */
public final class LuceneFilterRule {
  // TODO 1. Extend ConverterRule and add the appropriate constructor
  // TODO 2. Override ConverterRule#convert method and create a LuceneFilter
  // Don't forget to set the appropriate convention for the newly created filter
  // Don't forget to request the input to LuceneFilter to be in the same (LUCENE) convention
  // If you are stuck open EnumerableFilterRule and try to implement convert in a similar fashion.
  // If you are still stuck ask for help.
  // TODO 3. Override ConverterRule#matches method and ensure condition is of the appropriate form:
  // i) It is a RexCall
  // ii) The operator is EQUALS
  // iii) The left operand (0) is a input/column reference (RexInputRef)
  // iv) The right operand (1) is a literal value (RexLiteral) and the type is INTEGER
  // v) The input reference refers to table column; exploit RelMetadataQuery#getExpressionLineage
  // for this purpose; if it is not null then the expression can be tracked down to a table column
  // TODO 4. Create default rule configuration

}
