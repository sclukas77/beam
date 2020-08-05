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

package org.apache.beam.sdk.extensions.schemaio.expansion;

import com.google.auto.service.AutoService;
//import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.ServiceLoader;

@Experimental(Experimental.Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
public class ExternalSchemaIOTransformRegistrar implements ExternalTransformRegistrar {
    private static final String URN = "beam:external:java:schemaio:v1";
    private static ImmutableMap<String, SchemaIOProvider> IOPROVIDERS;

    public ExternalSchemaIOTransformRegistrar() {
        ImmutableMap.Builder builder = ImmutableMap.<String, SchemaIOProvider>builder();
            ServiceLoader<SchemaIOProvider> map = ServiceLoader.load(SchemaIOProvider.class);
            for (SchemaIOProvider schemaIOProvider : map) {
                builder = builder.put(schemaIOProvider.identifier(), schemaIOProvider);
            }
            IOPROVIDERS = builder.build();
    }


    @Override
    public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
        return null;
        //return ImmutableMap.of(URN, Builder.class); //both reader and writer??
        //map from above
        //loop in the service loader, create instances of transform builder with the given schemaIOprovider
    }

    @Override
    public Map<String, ExternalTransformBuilder> knownBuilderInstances() {
        ImmutableMap.Builder builder = ImmutableMap.<String, ExternalTransformRegistrar>builder();
            try {
                for(String id : IOPROVIDERS.keySet()) {
                    builder.put(id + ":reader", new ReaderBuilder(IOPROVIDERS.get(id)));
                    builder.put(id + ":writer", new WriterBuilder(IOPROVIDERS.get(id)));
                }
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
        return builder.build();
    }

    public static class Configuration {
        String location;
        byte[] config;
        byte[] dataSchema;

        public void setLocation(String location) { this.location = location; }

        public void setConfig(byte[] config) { this.config = config; }

        public void setDataSchema(byte[] dataSchema) { this.dataSchema = dataSchema; }
    }

    private static Schema translateSchema(byte[] schemaBytes) throws Exception {
        if(schemaBytes == null) {
            return null;
        }
        SchemaApi.Schema protoSchema = SchemaApi.Schema.parseFrom(schemaBytes);
        return SchemaTranslation.schemaFromProto(protoSchema);
    }

    private static Row translateRow(byte[] rowBytes, Schema configSchema) throws Exception{
        RowCoder rowCoder = RowCoder.of(configSchema);
        InputStream stream = new ByteArrayInputStream(rowBytes);
        return rowCoder.decode(stream);
    }

    private static class ReaderBuilder implements ExternalTransformBuilder<Configuration, PBegin, PCollection<Row>>{
        SchemaIOProvider schemaIOProvider;
        ReaderBuilder(SchemaIOProvider schemaIOProvider) {
            this.schemaIOProvider = schemaIOProvider;
        }

        @Override
        public PTransform<PBegin, PCollection<Row>> buildExternal(Configuration configuration) {
            try {
                Schema dataSchema = translateSchema(configuration.dataSchema);
                Row config = translateRow(configuration.config, schemaIOProvider.configurationSchema()); //need to pass in configSchema
                SchemaIO schemaIO = schemaIOProvider.from(configuration.location, config, dataSchema);
                PTransform<PBegin, PCollection<Row>> transform = schemaIO.buildReader();
                return transform;
            }
            catch (Exception e) {
                throw new RuntimeException("Could not convert configuration proto to row or schema.");
            }
        }
    }

    private static class WriterBuilder implements ExternalTransformBuilder<Configuration, PCollection<Row>, PDone> {
        SchemaIOProvider schemaIOProvider;
        WriterBuilder(SchemaIOProvider  schemaIOProvider) {
            this.schemaIOProvider = schemaIOProvider;
        }

        @Override
        public PTransform<PCollection<Row>, PDone> buildExternal(Configuration configuration) {
            try {
                Schema dataSchema = translateSchema(configuration.dataSchema);
                Row config = translateRow(configuration.config, schemaIOProvider.configurationSchema()); //need to pass in configSchema
                return (PTransform<PCollection<Row>, PDone>) schemaIOProvider
                        .from(configuration.location, config, dataSchema).buildWriter();
            }
            catch (Exception e) {
                throw new RuntimeException("Could not convert configuration proto to row or schema.");
            }
        }
    }
}