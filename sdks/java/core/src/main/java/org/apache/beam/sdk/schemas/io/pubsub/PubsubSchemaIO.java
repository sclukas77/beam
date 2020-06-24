package org.apache.beam.sdk.schemas.io.pubsub;

import org.apache.beam.sdk.annotations.Internal;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
//import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.io.gcp.pubsub.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import java.io.Serializable;

//import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.VARCHAR;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ROW;
import static org.apache.beam.sdk.schemas.io.pubsub.PubsubMessageToRow.*;

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

    //public method to call private constructor
    static PubsubSchemaIO withConfiguration(String location, Row config, Schema dataSchema) {
        return new PubsubSchemaIO(location, config, dataSchema);
    }

    //returns the schema of the data
    Schema schema() {
        return dataSchema;
    }

    //return something like a pubsubmessagetorow class
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
                                                //.useFlatSchema(!definesAttributeAndPayload(dataSchema))
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

    //return something like a rowtomessage class
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

        //get this from configuration row
        return config.getValue("timestampAttributeKey")
                ? read.withTimestampAttribute(config.getValue("timestampAttributeKey"))
                : read;
    }
    /*
    private boolean definesAttributeAndPayload(Schema schema) {
        return fieldPresent(
                schema, ATTRIBUTES_FIELD, Schema.FieldType.map(VARCHAR.withNullable(false), VARCHAR))
                && (schema.hasField(PAYLOAD_FIELD)
                && ROW.equals(schema.getField(PAYLOAD_FIELD).getType().getTypeName()));
    }

    private boolean fieldPresent(Schema schema, String field, Schema.FieldType expectedType) {
        return schema.hasField(field)
                && expectedType.equivalent(
                schema.getField(field).getType(), Schema.EquivalenceNullablePolicy.IGNORE);
    }*/

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
