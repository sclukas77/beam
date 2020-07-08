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

import java.io.Serializable;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BaseBeamTable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

public class SchemaIOTableWrapper extends BaseBeamTable implements Serializable {
  protected final SchemaIO schemaIO;

  private SchemaIOTableWrapper(SchemaIO schemaIO) {
    this.schemaIO = schemaIO;
  }

  static SchemaIOTableWrapper fromSchemaIO(SchemaIO schemaIO) {
    return new SchemaIOTableWrapper(schemaIO);
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.UNBOUNDED;
  }

  @Override
  public Schema getSchema() {
    return schemaIO.schema();
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    PTransform<PBegin, PCollection<Row>> readerTransform = schemaIO.buildReader();
    return begin.apply(readerTransform);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    PTransform<PCollection<Row>, POutput> writerTransform = schemaIO.buildWriter();
    return input.apply(writerTransform);
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return BeamTableStatistics.UNBOUNDED_UNKNOWN;
  }
}
