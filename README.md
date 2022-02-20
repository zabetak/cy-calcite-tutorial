<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# Building query compilers with Apache Calcite

A tutorial of [Apache Calcite]((http://calcite.apache.org))
for the course of [Data Integration](https://depinfo.u-cergy.fr/~vodislav/Master/IED/) at the
university of [Cergy-Pontoise](https://www.cyu.fr/).

In this tutorial, we demonstrate the main components of Calcite and how they interact with each
other. To do this we build, step-by-step, a fully fledged query processor for data residing
in multiple data sources. We chose Apache Lucene (NoSQL) and HyperSQL (SQL) as storage engines to
demonstrate how Calcite can be used on top of SQL and NoSQL systems.

The project contains the following modules:
* `indexer`, containing the necessary code to populate some sample dataset(s) into selected storage
engines to demonstrate the capabilities of the query processor;
* `processor`, containing the skeleton and documentation of selected classes, which the students can
use to follow the step by real-time implementation of the query processor.

## Requirements

* JDK version >= 8

## Quickstart

To compile the project, run:

    ./mvnw package -DskipTests 

To load/index the TPC-H dataset in Lucene and HyperSQL, run:

    java -jar indexer/target/indexer-1.0-SNAPSHOT-jar-with-dependencies.jar
    
The indexer creates the data under `target/tpch` directory. The TPC-H dataset was generated using
the dbgen command line utility (`dbgen -s 0.001`) provided in the original
[TPC-H tools](http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) bundle.

To execute SQL queries using the query processor run: 

    java -jar processor/target/processor-1.0-SNAPSHOT-jar-with-dependencies.jar queries/tpch/Q0.sql

Initially since the actual implementation of the query processor is missing the command will just
print the input query to standard output.

You can use one of the predefined queries under `queries/tpch` directory or create a new file
and write your own.
