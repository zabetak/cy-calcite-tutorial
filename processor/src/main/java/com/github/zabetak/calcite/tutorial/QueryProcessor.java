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
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

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
    // TODO 1. Create the root schema and type factory
    // TODO 2. Start creating the schema for Lucene by initializing an empty map to hold the tables
    // TODO 3. Obtain a data type builder from the type factory and create the data type for each
    // TPC-H table
    // TODO 4. Create a LuceneTable for each TPC-H table and add it to the map created above.
    // To find the index path (where the table is stored) check the DatasetLoader class.
    // TODO 5. Create a schema for Lucene and pass in the map with the lucene tables.
    // TODO 6. Register the lucene schema under the root with an appropriate name e.g., 'lucene'
    // TODO 7. Create an SQL parser for the provided sql query
    // TODO 8. Parse the query into an AST
    // TODO 9. Print and check the AST
    // TODO 10. Instantiate the catalog reader and configure it appropriately
    //  - Start with the DEFAULT CalciteConnectionConfig but inspect what others options are
    //  available cause you will need probably need to tune it a bit later on.
    // TODO 11. Instantiate an SQL validator using available utilities (SqlValidatorUtil)
    // - Use the standard operator table (find appropriate implementation of SqlOperatorTable)
    // - Use the default validator config
    // TODO 12. Validate the initial AST and store the valid AST in some variable
    // TODO 13. Create the optimization cluster to maintain planning information
    // TODO 14. Instantiate the converter of the AST to Logical plan and pass the appropriate
    //  parameters:
    // - No view expansion (use NOOP_EXPANDER)
    // - Standard expression normalization (use StandardConvertletTable.INSTANCE)
    // - Default configuration (SqlToRelConverter.config())
    // TODO 15. Convert the valid AST and obtain the root relational expression
    // TODO 16. Obtain the actual relational expression (RelNode) from the root
    // TODO 17. Display the logical plan using the optimizer utilities (RelOptUtil) class
    // TODO 18. Obtain the optimizer/planner from the cluster
    // TODO 19. Add the necessary rules to the planner
    // TODO 20. Request the type of the output plan (in this case we want a physical plan in
    // EnumerableConvention) using the changeTraits method.
    // TODO 21. Pass the resulting plan (with changed traits/properties) to planner#setRoot
    // TODO 22. Start the optimization process to obtain the most efficient physical plan based on
    // the provided rule set using the findBest method.
    // TODO 23. Display the physical plan
    // TODO 24. Try to understand why the CannotPlanException appears.
    // TODO 25. Goto to LuceneTable class and complete the missing bits to make the exception
    //  disappear
    // TODO 26. Pass the enumerable physical plan to compile method to generate the Java code and
    // obtain the executable plan.
    // TODO 27. Try to understand why the UnsupportedOperationException.
    // TODO 28. Add the necessary rules to the planner to avoid the exception.
    // Coding module II:
    // TODO 1. Implement the LuceneTableScan operator according to the instructions in the class.
    // TODO 2. Implement the LuceneTableScanRule operator according to the instructions in the class.
    // TODO 3. Register the LuceneTableScanRule to the planner.
    // TODO 4. Remove the EnumerableTableScanRule it is not needed anymore. This will lead to a
    // CannotPlanException that will tackle next. Can you spot why?
    // TODO 5. Got to LuceneToEnumerableConverter and try to understand what it does.
    // TODO 6. Implement LuceneToEnumerableConverterRule according to the instuctions in the class.
    // TODO 7. Register the LuceneToEnumerableConverterRule to the planner.
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
    return compile(null, null);
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
