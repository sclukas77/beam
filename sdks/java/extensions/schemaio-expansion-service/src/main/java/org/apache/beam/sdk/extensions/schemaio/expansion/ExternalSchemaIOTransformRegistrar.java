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
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.ServiceLoader;

@AutoService(ExternalTransformRegistrar.class)
public class ExternalSchemaIOTransformRegistrar implements ExternalTransformRegistrar {
    private static final String URN = "beam:external:java:schemaio:v1"; ////determines the type of environment
    private static ImmutableMap<String, Class<? extends SchemaIOProvider>> IOPROVIDERS;

    public ExternalSchemaIOTransformRegistrar() {
        ImmutableMap.Builder builder = ImmutableMap.<String, Class<? extends SchemaIOProvider>>builder();
        for (SchemaIOProvider schemaIOProvider : ServiceLoader.load(SchemaIOProvider.class)) {
          //  builder = builder.put(schemaIOProvider.identifier() + ":reader", schemaIOProvider);
          //  builder = builder.put(schemaIOProvider.identifier() + ":writer", schemaIOProvider);
            builder = builder.put(schemaIOProvider.identifier(), schemaIOProvider);
        }
        IOPROVIDERS = builder.build();
    }


    @Override
    public Map<String, Class<? extends ExternalTransformBuilder>> knownBuilders() {
        return null;
       // return ImmutableMap.of(URN, Builder.class); //both reader and writer??
        //map from above
        //loop in the service loader, create instances of transform builder with the given schemaIOprovider
    }

    @Override
    public Map<String, ExternalTransformRegistrar> knownBuilderInstances() throws IllegalAccessException, InstantiationException {
        ImmutableMap.Builder builder = ImmutableMap.<String, ExternalTransformRegistrar>builder();
        for(String urn : IOPROVIDERS.keySet()) {
           // String newURN = urn + ":reader";
            ////Builder readBuild = new ReaderBuilder(IOPROVIDERS.get(urn));//need to add in writer too
            builder.put(urn + ":reader", new ReaderBuilder(IOPROVIDERS.get(urn)));
            builder.put(urn + ":writer", new WriterBuilder(IOPROVIDERS.get(urn)));
        }
        return builder.build();
    }

    public static class Configuration {
        //general purpose reader
        //schema

        String location;
        byte[] config;
        byte[] dataSchema;

        public void setLocation(String location) { this.location = location; }

        public void configuration(byte[] config) { this.config = config; }

        public void dataSchema(byte[] dataSchema) { this.dataSchema = dataSchema; }
    }

    private static Schema translateSchema(byte[] schemaBytes) throws InvalidProtocolBufferException {
        SchemaApi.Schema protoSchema = SchemaApi.Schema.parseFrom(schemaBytes);
        return SchemaTranslation.schemaFromProto(protoSchema);
    }

    private static Row translateRow(byte[] rowBytes, Schema configSchema) throws InvalidProtocolBufferException{
        SchemaApi.Row protoRow = SchemaApi.Row.parseFrom(rowBytes);
        return (Row) SchemaTranslation.rowFromProto(protoRow, Schema.FieldType.row(configSchema));
    }

    private static class ReaderBuilder implements ExternalTransformBuilder<Configuration, PBegin, PCollection<Row>>{
        SchemaIOProvider schemaIOProvider;
        ReaderBuilder(Class<? extends SchemaIOProvider> schemaIOProvider) throws InstantiationException, IllegalAccessException {
            this.schemaIOProvider = schemaIOProvider.newInstance();
        }

        @Override
        public PTransform<PBegin, PCollection<Row>> buildExternal(Configuration configuration) {
            //how to instantiate SchemaIOProvider and SchemaIO?
            //return (new SchemaIOProvider()).from(...)
            //convert config.configuration and config.dataSchema to Row and Schema respectively using SchemaTranslation.java or proto
            try {
                Schema dataSchema = translateSchema(configuration.dataSchema);
                Row config = translateRow(configuration.config, schemaIOProvider.configurationSchema()); //need to pass in configSchema
                return schemaIOProvider.from(configuration.location, config, dataSchema).buildReader(); /////pbegin vs pinput casting
            }
            catch (InvalidProtocolBufferException e) {
                //what kind of error here?
                //throw new AssertionError("protobuf failed");
                e.printStackTrace();
            }
            return null;
        }
    }

    private static class WriterBuilder implements ExternalTransformBuilder<Configuration, PCollection<Row>, PDone> {
        SchemaIOProvider schemaIOProvider;
        WriterBuilder(Class<? extends SchemaIOProvider>  schemaIOProvider) throws InstantiationException, IllegalAccessException {
            this.schemaIOProvider = schemaIOProvider.newInstance();
        }

        @Override
        public PTransform<PCollection<Row>, PDone> buildExternal(Configuration configuration) {
            //need to do deserialization of byte array here
            try {
                Schema dataSchema = translateSchema(configuration.dataSchema);
                Row config = translateRow(configuration.config, schemaIOProvider.configurationSchema()); //need to pass in configSchema
                return (PTransform<PCollection<Row>, PDone>) schemaIOProvider
                        .from(configuration.location, config, dataSchema).buildWriter(); /////pbegin vs pinput casting
            }
            catch (InvalidProtocolBufferException e) {
                //what kind of error here?
                //throw new AssertionError("protobuf failed");
                e.printStackTrace();
            }
            return null;
        }
    }

        /*private static abstract class Builder implements ExternalTransformBuilder<Configuration, PInput, PCollection<Row>> {
        private SchemaIOProvider schemaIOProvider;

        /*Builder(Class<? extends SchemaIOProvider> schemaIOProvider) throws IllegalAccessException, InstantiationException {
            this.schemaIOProvider = (SchemaIOProvider) schemaIOProvider.newInstance();
        }

        //@Override
        //public abstract PTransform<PInput, PCollection<Row>> buildExternal(Configuration configuration);

        public Schema translateSchema(byte[] schemaBytes) throws InvalidProtocolBufferException {
            SchemaApi.Schema protoSchema = SchemaApi.Schema.parseFrom(schemaBytes);
            return SchemaTranslation.schemaFromProto(protoSchema);
        }

        public Row translateRow(byte[] rowBytes, Schema configSchema) throws InvalidProtocolBufferException{
            SchemaApi.Row protoRow = SchemaApi.Row.parseFrom(rowBytes);
            return (Row) SchemaTranslation.rowFromProto(protoRow, Schema.FieldType.row(configSchema));
        }
    }*/
}