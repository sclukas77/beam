package org.apache.beam.sdk.schemas.io.pubsub;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.SchemaCapableIOProvider;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nullable;
import java.io.Serializable;

//import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.VARCHAR;

@Internal
@AutoService(SchemaCapableIOProvider.class)
public class PubsubSchemaCapableIOProvider {
    //@Override
    /** Returns an id that uniquely represents this IO. */
    public String identifier() {
        return "pubsub";
    }

    /**
     * Returns the expected schema of the configuration object. Note this
     * is distinct from the schema of the data source itself.
     */
    //will be constant
    //need to make these values nullable
    public Schema configurationSchema() {
        return Schema.builder()
                .addNullableField("deadLetterQueue", FieldType.STRING)
                .addNullableField("timestampAttributeKey",FieldType.STRING)
                .build();
    }

    /**
     * Produce a SchemaIO given a String representing the data's
     * location, the schema of the data that resides there, and some
     * IO-specific configuration object.*/

    //needs to initialize a SchemaIO object
    public PubsubSchemaIO from(String location,
                  Row configuration,
                  Schema dataSchema) {
        return PubsubSchemaIO.withConfiguration(configuration, dataSchema);
    }

}
