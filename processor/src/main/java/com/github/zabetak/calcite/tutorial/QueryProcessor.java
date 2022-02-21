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
package com.github.zabetak.calcite.tutorial;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import com.github.zabetak.calcite.tutorial.indexer.DatasetLoader;
import com.github.zabetak.calcite.tutorial.indexer.TpchTable;
import com.github.zabetak.calcite.tutorial.rules.LuceneTableScanRule;
import com.github.zabetak.calcite.tutorial.rules.LuceneToEnumerableConverterRule;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Query processor for running TPC-H queries over Apache Lucene and HyperSQL.
 */
public class QueryProcessor {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: runner SQL_FILE");
      System.exit(-1);
    }
    String sqlQuery = new String(Files.readAllBytes(Paths.get(args[0])), StandardCharsets.UTF_8);
    long start = System.currentTimeMillis();
    for (Object row : execute(sqlQuery)) {
      if (row instanceof Object[]) {
        System.out.println(Arrays.toString((Object[]) row));
      } else {
        System.out.println(row);
      }
    }
    long finish = System.currentTimeMillis();
    System.out.println("Elapsed time " + (finish - start) + "ms");
  }

  /**
   * Plans and executes an SQL query.
   *
   * @param sqlQuery - a string with the SQL query for execution
   * @return an Enumerable with the results of the execution of the query
   * @throws SqlParseException if there is a problem when parsing the query
   */
  public static <T> Enumerable<T> execute(String sqlQuery)
      throws SqlParseException {
    System.out.println("[Input query]");
    System.out.println(sqlQuery);

    // Coding module I:
    CalciteSchema root = CalciteSchema.createRootSchema(false);
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    Map<String, Table> luceneTables = new HashMap<>();
    for (TpchTable table : TpchTable.values()) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (TpchTable.Column column : table.columns) {
        builder.add(column.name, typeFactory.createJavaType(column.type).getSqlTypeName());
      }
      RelDataType tableType = builder.build();
      String indexPath = DatasetLoader.LUCENE_INDEX_PATH.resolve(table.name()).toString();
      luceneTables.put(table.name(), new LuceneTable(indexPath, tableType));
    }
    LuceneSchema luceneSchema = new LuceneSchema(luceneTables);
    root.add("lucene", luceneSchema);
    SqlParser parser = SqlParser.create(sqlQuery);
    SqlNode astNode = parser.parseQuery();
    System.out.println("[Parsed query]");
    System.out.println(astNode);
    CalciteConnectionConfig readerConf = CalciteConnectionConfig.DEFAULT
        .set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
    CalciteCatalogReader reader = new CalciteCatalogReader(root, Collections.emptyList(), typeFactory, readerConf);
    SqlValidator sqlValidator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
        reader,
        typeFactory,
        SqlValidator.Config.DEFAULT);
    SqlNode validAst = sqlValidator.validate(astNode);
    System.out.println("[Validated query]");
    System.out.println(validAst);
    RelOptCluster cluster = newCluster(typeFactory);
    SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(NOOP_EXPANDER, sqlValidator,
        reader, cluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config());
    RelRoot relRoot = sqlToRelConverter.convertQuery(validAst, false, true);
    RelNode logicalPlan = relRoot.rel;
    System.out.println("[Logical plan]");
    System.out.println(RelOptUtil.toString(logicalPlan));
    RelOptPlanner planner = cluster.getPlanner();
    planner.addRule(CoreRules.PROJECT_TO_CALC);
    planner.addRule(CoreRules.FILTER_TO_CALC);
    planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    planner.addRule(LuceneTableScanRule.DEFAULT.toRule());
    planner.addRule(LuceneToEnumerableConverterRule.DEFAULT.toRule());
    logicalPlan = planner.changeTraits(logicalPlan,
        logicalPlan.getTraitSet().replace(EnumerableConvention.INSTANCE));
    planner.setRoot(logicalPlan);
    RelNode physicalPlan = planner.findBestExp();
    System.out.println("[Physical plan]");
    System.out.println(RelOptUtil.toString(physicalPlan));
    return compile(root, physicalPlan);
    // Coding module III:
    // TODO 1. Use the JdbcSchema class to create a data source for establishing connections to
    //  HyperSQL. You can find the appropriate url, username, etc., to use in DatasetLoader.
    // TODO 2. Create a JdbcSchema using one of the available static factory methods.
    // Tip: you can turn CalciteSchema to SchemaPlus by calling the plus() method.
    // TODO 3. Register the jdbc schema under the root with an appropriate name e.g., 'hyper'
    // TODO 4. Run a query and explain what happens. Where are the data coming from?
    // TODO 5. Do the appropriate changes to fetch data from both Lucene, and HyperSQL.
    // TODO 6. Explain where is the JdbcConvention and where are the JdbcRules.
    // Coding module IV:
    // TODO 7. Implement the LuceneFilter operator according to the instructions in the class.
    // TODO 8. Implement the LuceneFilterRule according to the instructions in the class.
    // TODO 9. Add the LuceneFilterRule to the planner, observer the physical plan and explain what
    // happens.
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  private static <T> Enumerable<T> compile(CalciteSchema schema, RelNode phyPlan) {
    if(schema == null) {
      System.err.println("No schema found. Return empty results");
      return Linq4j.emptyEnumerable();
    }
    if(phyPlan == null) {
      System.err.println("No physical plan found. Return empty results");
      return Linq4j.emptyEnumerable();
    }
    Bindable bindable = EnumerableInterpretable.toBindable(
        Collections.emptyMap(), null, (EnumerableRel) phyPlan, EnumerableRel.Prefer.ARRAY);
    return bindable.bind(new SchemaOnlyDataContext(schema));
  }

  private static final RelOptTable.ViewExpander NOOP_EXPANDER = (type, query, schema, path) -> null;

  /**
   * A simple data context only with schema information.
   */
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return new JavaTypeFactoryImpl();
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}
