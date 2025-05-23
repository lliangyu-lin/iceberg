/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.InteropTestSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRunner {

  protected static SparkSession spark =
      SparkSession.builder()
          .appName("Iceberg Cross-Client Producer")
          .master("local[2]")
          .config("spark.sql.catalog.interop", "org.apache.iceberg.spark.SparkCatalog")
          .config("spark.sql.catalog.interop.type", "rest")
          .config("spark.sql.catalog.interop.uri", "http://localhost:8181")
          .config("spark.sql.defaultCatalog", "interop")
          .getOrCreate();

  @Test
  public void runProducer() throws IOException {
    File jsonFile =
        new File(
            "/Users/lliangyu/Desktop/Repository/iceberg/api/src/test/resources/producer_simple_test.json");
    ObjectMapper mapper = new ObjectMapper();
    InteropTestSpec spec = mapper.readValue(jsonFile, InteropTestSpec.class);

    //    spark.sql("CREATE DATABASE default").show();
    spark.sql("SHOW DATABASES").show();

    for (InteropTestSpec.Action action : spec.actions) {
      if (action.type.equals("create_table")) {
        System.out.println("Run create table...");
        String table = action.tableName;
        List<String> columnDefs = Lists.newArrayList();
        for (InteropTestSpec.Field field : action.schema.fields) {
          columnDefs.add(String.format("`%s` %s", field.name, field.type));
        }
        String schemaDDL = String.join(", ", columnDefs);

        String sql = String.format("CREATE TABLE %s (%s) USING iceberg", table, schemaDDL);
        System.out.println("Executing SQL: " + sql);
        spark.sql(sql);
      } else if (action.type.equals("insert_rows")) {
        spark.sql("SHOW TABLES FROM default").show();
        System.out.println("Run insert...");
        String table = action.tableName;

        // Build SQL string like: INSERT INTO interop.default.insert_test VALUES (1, 'Alice'), ...
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(table).append(" VALUES ");

        List<String> valueStrings = Lists.newArrayList();
        for (Map<String, Object> row : action.rows) {
          List<String> literals = Lists.newArrayList();
          for (Object value : row.values()) {
            if (value instanceof String) {
              literals.add("'" + value.toString().replace("'", "''") + "'");
            } else {
              literals.add(value.toString());
            }
          }
          valueStrings.add("(" + String.join(", ", literals) + ")");
        }

        sb.append(String.join(", ", valueStrings));
        String sql = sb.toString();

        System.out.println("Executing SQL: " + sql);
        spark.sql(sql);
      }
    }
  }

  @Test
  public void runConsumer() throws IOException {
    File jsonFile =
        new File(
            "/Users/lliangyu/Desktop/Repository/iceberg/api/src/test/resources/consumer_simple_test.json");
    ObjectMapper mapper = new ObjectMapper();
    InteropTestSpec spec = mapper.readValue(jsonFile, InteropTestSpec.class);

    Map<String, String> catalogProps =
        Map.of(
            "type", "rest",
            "uri", "http://localhost:8181");
    Catalog catalog = CatalogUtil.buildIcebergCatalog("interop", catalogProps, null);

    for (InteropTestSpec.Action action : spec.actions) {
      if (action.type.equals("scan_table")) {
        Table table = catalog.loadTable(TableIdentifier.parse(action.tableName));
        TableScan scan = table.newScan();

        long totalRecords = 0;
        List<Record> allRecords = Lists.newArrayList();
        FileIO io = table.io();

        for (FileScanTask task : scan.planFiles()) {
          InputFile inputFile = io.newInputFile(task.file().path().toString());
          final CloseableIterable<Record> reader =
              Parquet.read(inputFile)
                  .project(table.schema())
                  .createReaderFunc(
                      fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                  .build();
          try (CloseableIterable<Record> records = reader) {
            for (Record record : records) {
              allRecords.add(record);
              totalRecords++;
            }
          }
        }

        Assertions.assertEquals(2, totalRecords);

        // Optional: Validate individual records
        //        if (action.expect.records != null && !action.expect.records.isEmpty()) {
        //          List<Map<String, Object>> expected = action.expect.records;
        //          List<Map<String, Object>> actual = Lists.newArrayList();
        //          for (Record r : allRecords) {
        //            Map<String, Object> map = Maps.newHashMap();
        //            for (Types.NestedField field : table.schema().columns()) {
        //              map.put(field.name(), r.getField(field.name()));
        //            }
        //            actual.add(map);
        //          }
        //
        //          for (Map<String, Object> exp : expected) {
        //            boolean matched = actual.stream().anyMatch(act -> act.equals(exp));
        //            if (!matched) {
        //              throw new AssertionError("Expected row not found: " + exp);
        //            }
        //          }
        //        }

        System.out.println("scan_table passed for " + action.tableName);
      }
    }
  }
}
