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
package org.apache.beam.sdk.io.gcp.pubsub;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.DLQ_TAG;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.MAIN_TAG;

@Internal
public class PubsubSchemaIO implements Serializable {
    protected final Row config;
    protected final Schema dataSchema;
    protected final String location;

    private PubsubSchemaIO(String location, Row config, Schema dataSchema) {
        this.config = config;
        this.dataSchema = dataSchema;
        this.location = location;
    }

    static PubsubSchemaIO withConfiguration(String location, Row config, Schema dataSchema) {
        return new PubsubSchemaIO(location, config, dataSchema);
    }

    Schema schema() {
        return dataSchema;
    }

    public PTransform<PBegin, PCollection<Row>> buildReader() {
        return new PTransform<PBegin, PCollection<Row>>() {
            @Override
            public PCollection<Row> expand(PBegin begin) {
                PCollectionTuple rowsWithDlq =
                        begin
                                .apply("ReadFromPubsub", readMessagesWithAttributes())
                                .apply(
                                        "PubsubMessageToRow",
                                        PubsubMessageToRow.builder()
                                                .messageSchema(dataSchema)
                                                .useDlq(useDlqCheck(config))
                                                .useFlatSchema(config.getBoolean("useFlatSchema"))
                                                .build());
                rowsWithDlq.get(MAIN_TAG).setRowSchema(dataSchema);

                if (useDlqCheck(config)) {
                    rowsWithDlq.get(DLQ_TAG).apply(writeMessagesToDlq());
                }

                return rowsWithDlq.get(MAIN_TAG);
            }


        };

    }

    public PTransform<PCollection<Row>, POutput> buildWriter() {
        if(!config.getBoolean("useFlatSchema")) {
            throw new UnsupportedOperationException(
                    "Writing to a Pubsub topic is only supported for flattened schemas");
        }

        return new PTransform<PCollection<Row>, POutput>() {
            @Override
            public POutput expand(PCollection<Row> input) {
                return input
                        .apply(RowToPubsubMessage.fromTableConfig(config))
                        .apply(createPubsubMessageWrite());
            }
        };

    }

    private PubsubIO.Read<PubsubMessage> readMessagesWithAttributes() {
        PubsubIO.Read<PubsubMessage> read =
                PubsubIO.readMessagesWithAttributes().fromTopic(location);

        return useTimestampAttribute(config)
                ? read.withTimestampAttribute(config.getValue("timestampAttributeKey"))
                : read;
    }

    private PubsubIO.Write<PubsubMessage> writeMessagesToDlq() {
        PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(config.getString("deadLetterQueue"));

        return useTimestampAttribute(config)
                ? write.withTimestampAttribute(config.getString("timestampAttributeKey"))
                : write;
    }

    private boolean useDlqCheck(Row config) {
        return config.getValue("deadLetterQueue")!=null;
    }

    private boolean useTimestampAttribute(Row config) {
        return config.getValue("timestampAttributeKey")!=null;
    }

    private PubsubIO.Write<PubsubMessage> createPubsubMessageWrite() {
        PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(location);
        if (useTimestampAttribute(config)) {
            write = write.withTimestampAttribute(config.getValue("timestampAttributeKey"));
        }
        return write;
    }
}
