/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.meta.provider;

import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.InvalidSchemaException;
import org.apache.beam.sdk.schemas.io.SchemaCapableIOProvider;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.values.Row;

public abstract class SchemaCapableIOTableProviderWrapper extends InMemoryMetaTableProvider {
  private SchemaCapableIOProvider schemaCapableIOProvider;

  public SchemaCapableIOTableProviderWrapper(SchemaCapableIOProvider schemaCapableIOProvider) {
    this.schemaCapableIOProvider = schemaCapableIOProvider;
  }

  @Override
  public String getTableType() {
    return schemaCapableIOProvider.identifier();
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table tableDefinition) {
    JSONObject tableProperties = tableDefinition.getProperties();

    try {
      RowJson.RowJsonDeserializer deserializer =
          RowJson.RowJsonDeserializer.forSchema(schemaCapableIOProvider.configurationSchema())
              .withNullBehavior(RowJson.RowJsonDeserializer.NullBehavior.ACCEPT_MISSING_OR_NULL);

      Row configurationRow =
          newObjectMapperWith(deserializer).readValue(tableProperties.toString(), Row.class);

      SchemaIO schemaIO =
          schemaCapableIOProvider.from(
              tableDefinition.getLocation(), configurationRow, tableDefinition.getSchema());

      return SchemaIOTableWrapper.fromSchemaIO(schemaIO);
    } catch (InvalidConfigurationException | InvalidSchemaException e) {
      throw new InvalidTableException(e.getMessage());
    } catch (JsonProcessingException e) {
      throw new AssertionError(
          "Failed to re-parse TBLPROPERTIES JSON " + tableProperties.toString());
    }
  }
}
