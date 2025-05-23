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
package org.apache.iceberg.data;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Action;
import org.apache.iceberg.ExecutionContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestRunner {

  private static Path testSpec;

  @BeforeAll
  public static void beforeAll() {
    String specPath = System.getProperty("specPath");
    if (specPath == null) {
      throw new IllegalArgumentException("Missing -DspecPath");
    }

    testSpec = Path.of(specPath);
  }

  @Test
  public void test() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> specJson = mapper.readValue(testSpec.toFile(), new TypeReference<>() {});
    List<Action> actions = mapper.convertValue(specJson.get("actions"), new TypeReference<>() {});

    ExecutionContext ctx = new ExecutionContext("/tmp/iceberg");

    for (Action action : actions) {
      action.execute(ctx);
    }
  }
}
