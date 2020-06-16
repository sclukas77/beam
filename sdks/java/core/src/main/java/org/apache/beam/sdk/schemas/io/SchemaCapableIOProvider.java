package org.apache.beam.sdk.schemas.io;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

public interface SchemaCapableIOProvider {
    /** Returns an id that uniquely represents this IO. */
    String identifier();

    /**
     * Returns the expected schema of the configuration object. Note this
     * is distinct from the schema of the data source itself.
     */
    Schema configurationSchema();

    /**
     * Produce a SchemaIO given a String representing the data's
     * location, the schema of the data that resides there, and some
     * IO-specific configuration object.
     */
    SchemaIO from(String location,
                  Row configuration,
                  Schema dataSchema);
}
