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
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
/**
 * An implementation of {@link SchemaIOProvider} for reading and writing JSON payloads with {@link
 * JdbcIO}.
 */
class JdbcSchemaIOProvider implements SchemaIOProvider {

    /** Returns an id that uniquely represents this IO. */
    @Override
    public String identifier() {
        return "jdbc";
    }

    /**
     * Returns the expected schema of the configuration object. Note this is distinct from the schema
     * of the data source itself.
     */
    @Override
    public Schema configurationSchema() {
        return Schema.builder()
                .build();
    }

    /**
     * Produce a SchemaIO given a String representing the data's location, the schema of the data that
     * resides there, and some IO-specific configuration object.
     */
    @Override
    public JdbcSchemaIO from(String location, Row configuration, Schema dataSchema) {
        return new JdbcSchemaIO(location, configuration);
    }

    @Override
    public boolean requiresDataSchema() {
        return false;
    }

    @Override
    public PCollection.IsBounded isBounded() {
        return PCollection.IsBounded.UNBOUNDED; ///////////bounded or no?
    }


    /** An abstraction to create schema aware IOs. */
    static class JdbcSchemaIO implements SchemaIO, Serializable {
        protected final Row config;
        protected final String location;

        JdbcSchemaIO(String location, Row config) {
            this.config = config;
            this.location = location;
        }

        @Override
        public Schema schema() {
            return null;
        }

        @Override
        public PTransform<PBegin, PCollection<Row>> buildReader() {
            ReadConfiguration readConfig = new AutoValueSchema()
                    .fromRowFunction(TypeDescriptor.of(ReadConfiguration.class))
                    .apply(config);
            JdbcIO.DataSourceConfiguration dataSourceConfiguration = readConfig.getDataSourceConfiguration();

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
            WriteConfiguration writeConfig = new AutoValueSchema()
                    .fromRowFunction(TypeDescriptor.of(WriteConfiguration.class))
                    .apply(config);
            JdbcIO.DataSourceConfiguration dataSourceConfiguration = writeConfig.getDataSourceConfiguration();

            // TODO: BEAM-10396 use writeRows() when it's available
            return JdbcIO.<Row>write()
                    .withDataSourceConfiguration(dataSourceConfiguration)
                    .withStatement(writeConfig.statement)
                    .withPreparedStatementSetter(new JdbcUtil.BeamRowPreparedStatementSetter());
        }
    }
}
