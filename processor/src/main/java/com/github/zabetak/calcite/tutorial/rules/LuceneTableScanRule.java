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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

import com.github.zabetak.calcite.tutorial.LuceneTable;
import com.github.zabetak.calcite.tutorial.operators.LuceneRel;
import com.github.zabetak.calcite.tutorial.operators.LuceneTableScan;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;

/**
 * Rule to convert a {@link LogicalTableScan} to a {@link LuceneTableScan} if possible.
 * The expression can be converted to a {@link LuceneTableScan} if the table corresponds to a Lucene
 * index.
 */
public final class LuceneTableScanRule extends ConverterRule {
  public LuceneTableScanRule(Config config) {
    super(config);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    LuceneTable tbl = rel.getTable().unwrap(LuceneTable.class);
    if(tbl == null) {
      return null;
    }
    return new LuceneTableScan(rel.getCluster(),
        rel.getTraitSet().replace(LuceneRel.LUCENE),
        Collections.emptyList(),
        rel.getTable());
  }
  
  public static final Config DEFAULT = Config.INSTANCE
      .withConversion(LogicalTableScan.class, Convention.NONE, LuceneRel.LUCENE, "LuceneTableScanRule")
      .withRuleFactory(LuceneTableScanRule::new);

}
