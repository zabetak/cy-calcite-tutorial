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

import org.apache.calcite.rel.logical.LogicalTableScan;

import com.github.zabetak.calcite.tutorial.operators.LuceneTableScan;

/**
 * Rule to convert a {@link LogicalTableScan} to a {@link LuceneTableScan} if possible.
 * The expression can be converted to a {@link LuceneTableScan} if the table corresponds to a Lucene
 * index.
 */
public final class LuceneTableScanRule {
  // TODO 1. Extend Converter rule and add the appropriate constructor
  // TODO 2. Implement convert method and leave empty for now
  // TODO 3. Create default rule config starting from ConverterRule.Config.INSTANCE
  // - Use withConversion method to i) specify which operator you want to match, ii) what should be
  // the input convention, iii) what should be the output convention, iv) name of the rule
  // - Use withRuleFactory method to indicate which method should be called to create the rule (hint
  // lamda expression to the constructor).
  // TODO 4. Continue with the implementation of the convert method.
  // Check the class of the table is the appropriate one; if not return null
  // Create a LuceneTableScan with the appropriate traitset. You need to change the convention.Do
  // you remember how to change traits?You have seen it before
  // TODO 5. Go back to the processor and register this rule to the planner.

}
