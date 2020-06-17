package org.apache.beam.sdk.schemas.io.pubsub;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;

@Internal
public class PubsubSchemaIO implements Serializable {
    protected final Row config;
    protected final Schema dataSchema;

    private PubsubSchemaIO(Row config, Schema dataSchema) {
        this.config = config;
        this.dataSchema = dataSchema;
    }

    //public method to call private constructor
    static PubsubSchemaIO withConfiguration(Row config, Schema dataSchema) {
        return new PubsubSchemaIO(config, dataSchema);
    }

    //returns the schema of the data
    Schema schema() {
        return dataSchema;
    }

    //return something like a pubsubmessagetorow class

    //to finish. expand() should resolve the errors here
    /*public PTransform<PBegin, PCollection<Row>> buildReader() {
        return new PTransform<PBegin, PCollection<Row>>() {
            @Override
            public PCollection<Row> expand(PBegin begin) {
                PCollectionTuple rowsWithDlq =
                        begin
                                .apply("ReadFromPubsub", readMessagesWithAttributes())
                                .apply(
                                        "PubsubMessageToRow",
                                        PubsubMessageToRow.builder()
                                                .messageSchema(getSchema())
                                                .useDlq(config.useDlq())
                                                .useFlatSchema(config.getUseFlatSchema())
                                                .build());
                rowsWithDlq.get(MAIN_TAG).setRowSchema(getSchema());

                if (config.useDlq()) {
                    rowsWithDlq.get(DLQ_TAG).apply(writeMessagesToDlq());
                }

                return rowsWithDlq.get(MAIN_TAG);
            }


        };

    }

    //return something like a rowtomessage class
  /*  PTransform<PCollection<Row>, POutput> buildWriter() {

    }*/
}
