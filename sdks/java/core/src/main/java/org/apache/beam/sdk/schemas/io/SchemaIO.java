package org.apache.beam.sdk.schemas.io;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

public interface SchemaIO {
    Schema schema();

    PTransform<PBegin, PCollection<Row>> buildReader();
    PTransform<PCollection<Row>, POutput> buildWriter();
}
