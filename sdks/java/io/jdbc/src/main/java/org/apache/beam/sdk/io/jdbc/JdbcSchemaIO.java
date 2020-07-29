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
package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.io.jdbc.JdbcReadRowsRegistrar.ReadConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcWriteRegistrar.WriteConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;

class JdbcSchemaIO implements SchemaIO, Serializable {
    protected final CrossLanguageConfiguration config;

    JdbcSchemaIO(CrossLanguageConfiguration config) {
        this.config = config;
    }

    @Override
    public Schema schema() {
        return null;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
        ReadConfiguration readConfig = (ReadConfiguration) config;
        JdbcIO.DataSourceConfiguration dataSourceConfiguration = config.getDataSourceConfiguration();

        JdbcIO.ReadRows readRows =
                JdbcIO.readRows()
                        .withDataSourceConfiguration(dataSourceConfiguration)
                        .withQuery(readConfig.query);

        if (readConfig.fetchSize != null) {
            readRows = readRows.withFetchSize(readConfig.fetchSize);
        }
        if (readConfig.outputParallelization != null) {
            readRows = readRows.withOutputParallelization(readConfig.outputParallelization);
        }
        return readRows;
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildWriter() {
        WriteConfiguration writeConfig = (WriteConfiguration) config;
        JdbcIO.DataSourceConfiguration dataSourceConfiguration = config.getDataSourceConfiguration();

        // TODO: BEAM-10396 use writeRows() when it's available
        return JdbcIO.<Row>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withStatement(writeConfig.statement)
                .withPreparedStatementSetter(new JdbcUtil.BeamRowPreparedStatementSetter());
    }
}
