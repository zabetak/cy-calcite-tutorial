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
import org.apache.calcite.rex.RexNode;
import org.apache.lucene.search.Query;

/**
 * Implementation of {@link Filter} in {@link LuceneRel#LUCENE} convention.
 *
 * The expression knows how to transform a filter condition in Calcite's {@link RexNode}
 * representation to the respective {@link Query} object in Lucene.
 */
public class LuceneFilter {
  // TODO 1. Extend Filter operator and add an appropriate constuctor
  // TODO 2. What does the constructor need to ensure about the arguments?
  // TODO 3. Implement Filter#copy method, everything you need is in the parameters of the method
  // TODO 4. Implement LuceneRel interface
  // TODO 5. Implement LuceneRel#implement method
  // You have to call implement() to the input of this operator and combine the results. This
  // operator does not know where is the Lucene index; you can obtain this information from the
  // input.
  // In order to obtain the query you have to transform the condition expressed as RexNode tree
  // to a Lucene Query object. Use the RexToLuceneTranslator to do this.
  // TODO 6. Complete the RexToLuceneTranslator to be able to transform a RexNode to a Query.
}
