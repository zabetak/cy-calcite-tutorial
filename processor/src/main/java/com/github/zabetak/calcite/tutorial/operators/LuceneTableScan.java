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

import org.apache.calcite.rel.core.TableScan;

/**
 * Implementation of {@link TableScan} in {@link LuceneRel#LUCENE} convention.
 *
 * The expression knows where is the Lucene index located and how to access it.
 */
public final class LuceneTableScan {
  // TODO 1. Extend TableScan operator
  // TODO 2. Add a constructor accepting at least a cluster, traitset, and table
  // TODO 3. What does the constructor need to ensure about the arguments?
  // TODO 4. Implement LuceneRel interface
  // TODO 5. Implement missing methods
  // TODO 6. Return an appropriate result from the implement method
  // - Use getTable().unwrap(...) to obtain the appropriate object and get the path to the index
  // - Use a Lucene query object for scanning all the table (the equivalent of "*:*")

}
