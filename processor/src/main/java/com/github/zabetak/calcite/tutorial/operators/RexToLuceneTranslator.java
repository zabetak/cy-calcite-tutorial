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
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.search.Query;

import java.util.Set;

/**
 * Translate row expressions ({@link org.apache.calcite.rex.RexNode}) to Apache Lucene queries
 * ({@link Query}).
 *
 * The translator assumes the expression is in some normalized form. At its current state it cannot
 * translate arbitrary Calcite expressions.
 */
public final class RexToLuceneTranslator extends RexVisitorImpl<Query> {
  private final Filter filter;

  private RexToLuceneTranslator(Filter filter) {
    super(false);
    this.filter = filter;
  }

  @Override public Query visitCall(RexCall call) {
    if(SqlKind.EQUALS.equals(call.getKind())){
      RexInputRef ref = (RexInputRef) call.operands.get(0);
      RexLiteral literal = (RexLiteral) call.operands.get(1);
      RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
      Set<RexNode> lineage = mq.getExpressionLineage(filter.getInput(), ref);
      RexTableInputRef tableInputRef = lineage.stream().findFirst().map(RexTableInputRef.class::cast).get();
      RelDataTypeField column = tableInputRef.getTableRef()
          .getTable()
          .getRowType()
          .getFieldList()
          .get(tableInputRef.getIndex());
      return IntPoint.newExactQuery(column.getName(), literal.getValueAs(Integer.class));
    }
    throw new IllegalStateException("Transformation not supported");
  }

  /**
   * Translates the condition in the specified filter to a Lucene query.
   */
  public static Query translate(Filter filter) {
    RexToLuceneTranslator translator = new RexToLuceneTranslator(filter);
    return filter.getCondition().accept(translator);
  }
}
