/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache;

import java.util.List;
import java.util.Map;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.Action;
import org.apache.iceberg.ExecutionContext;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class CreateTableAction implements Action {
  public String tableId;
  public List<SchemaField> schema;
  public String location;
  public Map<String, Object> partitionSpec;
  public Map<String, String> properties;

  public static class SchemaField {
    public String name;
    public String type;
    public boolean required;
    public String doc;
  }

  @Override
  public void execute(ExecutionContext context) {
    List<Types.NestedField> fields = Lists.newArrayList();
    for (int i = 0; i < schema.size(); i++) {
      SchemaField f = schema.get(i);
      Type parsedType = Types.fromPrimitiveString(f.type);
      Types.NestedField nf = f.required
              ? Types.NestedField.required(i, f.name, parsedType, f.doc)
              : Types.NestedField.optional(i, f.name, parsedType, f.doc);
      fields.add(nf);
    }

    Schema icebergSchema = new Schema(fields);
    PartitionSpec spec = PartitionSpec.unpartitioned(); // TODO: parse from JSON if needed

    TableIdentifier ident = TableIdentifier.parse(tableId);
    context.catalog().createTable(ident, icebergSchema, spec, location == null || location.isEmpty() ? null : location, properties);
  }
}
