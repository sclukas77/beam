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
package org.apache.beam.sdk.extensions.sql.meta.provider.avro;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/** {@link AvroTable} is a {@link BeamSqlTable}. */
public class AvroTable extends SchemaBaseBeamTable implements Serializable {
  private final SchemaIO avroSchemaIO;

  public AvroTable(SchemaIO avroSchemaIO) {
    super(avroSchemaIO.schema());
    this.avroSchemaIO = avroSchemaIO;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    PTransform<PBegin, PCollection<Row>> readerTransform = avroSchemaIO.buildReader();
    return begin.apply(readerTransform);
  }

  @Override
  public PDone buildIOWriter(PCollection<Row> input) {
    PTransform<PCollection<Row>, POutput> writerTransform = avroSchemaIO.buildWriter();
    return (PDone) input.apply(writerTransform);
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return BeamTableStatistics.BOUNDED_UNKNOWN;
  }
}
