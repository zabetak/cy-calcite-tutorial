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
package com.github.zabetak.calcite.tutorial.operators;

import org.apache.calcite.rel.core.Filter;
import org.apache.lucene.search.Query;

/**
 * Translate row expressions ({@link org.apache.calcite.rex.RexNode}) to Apache Lucene queries
 * ({@link Query}).
 *
 * The translator assumes the expression is in some normalized form. At its current state it cannot
 * translate arbitrary Calcite expressions.
 */
public final class RexToLuceneTranslator {
  // TODO 1. Extend RexVisitorImpl<Query> and call super with the right argument
  // TODO 2. Override and implement visitCall
  // TODO 3. Ensure call.getKind() corresponds to equals operator
  // TODO 4. Obtain an input/column reference from left operand (0) and cast to RexInputRef
  // TODO 5. Obtain literal from right operand (1) and cast to RexLiteral
  // TODO 6. Obtain RelMetadataQuery from cluster
  // TODO 7. Use RelMetadataQuery#getExpressionLineage on filter.getInput() to find the table column
  // TODO 8. Obtain the first expression from the lineage and cast it to RexTableInputRef
  // TODO 9. Obtain RelDataTypeField, which has the column name, using the information in
  // RelTableInputRef (ask for help if you reach this point and get stuck)
  // TODO 10. Check the data type is the INTEGER and create the appropriate Lucene
  // query (use Lucene's IntPoint class)
  // TODO 11. Throw assertion/exception if it happens to encounter an expression we cannot handle
  // TODO 12. Remove the assertion error from translate and apply the translator to the filter
  //  condition.
  private final Filter filter;

  private RexToLuceneTranslator(Filter filter) {
    this.filter = filter;
  }

  /**
   * Translates the condition in the specified filter to a Lucene query.
   */
  public static Query translate(Filter filter) {
    RexToLuceneTranslator translator = new RexToLuceneTranslator(filter);
    return null;
  }
}
